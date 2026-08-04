import time
from math import inf

from .. import _core
from .._abc import Clock
from .._util import final
from ._run import GLOBAL_RUN_CONTEXT

################################################################
# The glorious MockClock
################################################################


# Prior art:
#   https://twistedmatrix.com/documents/current/api/twisted.internet.task.Clock.html
#   https://github.com/ztellman/manifold/issues/57
@final
class MockClock(Clock):
    """A user-controllable clock suitable for writing tests.

    Args:
      rate (float): the initial :attr:`rate`.
      autojump_threshold (float): the initial :attr:`autojump_threshold`.
      autojump_wait_for_threads (bool): the initial
          :attr:`autojump_wait_for_threads`.

    .. attribute:: rate

       How many seconds of clock time pass per second of real time. Default is
       0.0, i.e. the clock only advances through manuals calls to :meth:`jump`
       or when the :attr:`autojump_threshold` is triggered. You can assign to
       this attribute to change it.

    .. attribute:: autojump_threshold

       The clock keeps an eye on the run loop, and if at any point it detects
       that all tasks have been blocked for this many real seconds (i.e.,
       according to the actual clock, not this clock), then the clock
       automatically jumps ahead to the run loop's next scheduled
       timeout. Default is :data:`math.inf`, i.e., to never autojump. You can
       assign to this attribute to change it.

       By default, a task that's waiting on a `trio.to_thread.run_sync`
       worker thread does not count as blocked for this purpose — the clock
       waits for the thread instead of jumping ahead of it. See
       :attr:`autojump_wait_for_threads`.

       Basically the idea is that if you have code or tests that use sleeps
       and timeouts, you can use this to make it run much faster, totally
       automatically. (At least, as long as those sleeps/timeouts are
       happening inside Trio; if your test involves talking to external
       service and waiting for it to timeout then obviously we can't help you
       there.)

       You should set this to the smallest value that lets you reliably avoid
       "false alarms" where some I/O is in flight (e.g. between two halves of
       a socketpair) but the threshold gets triggered and time gets advanced
       anyway. This will depend on the details of your tests and test
       environment. If you aren't doing any I/O (like in our sleeping example
       above) then just set it to zero, and the clock will jump whenever all
       tasks are blocked.

       .. note:: If you use ``autojump_threshold`` and
          `wait_all_tasks_blocked` at the same time, then you might wonder how
          they interact, since they both cause things to happen after the run
          loop goes idle for some time. The answer is:
          `wait_all_tasks_blocked` takes priority. If there's a task blocked
          in `wait_all_tasks_blocked`, then the autojump feature treats that
          as active task and does *not* jump the clock.

    .. attribute:: autojump_wait_for_threads

       Whether the autojump feature should also wait for work that's in
       flight outside the run loop – most commonly a worker thread spawned by
       `trio.to_thread.run_sync` – before jumping the clock. Default is
       True. You can assign to this attribute to change it.

       A task that's waiting on a worker thread looks "blocked" to the run
       loop, but the thread is making real progress in real time; jumping
       virtual time ahead of it tends to fire timeouts spuriously. So by
       default, the clock does not autojump while such work is running, and
       instead waits (in real time) for it to finish.

       More precisely, this controls whether the autojump feature respects
       *autojump holds* (see `trio.lowlevel.autojump_hold`), which
       `trio.to_thread.run_sync` acquires around its worker threads and
       which other libraries can acquire around worker processes and the
       like. A thread that's blocked calling back into Trio via
       `trio.from_thread.run` doesn't hold the clock — virtual time can
       still advance while the delivered async function runs.

       .. warning:: If a thread can only finish once virtual time advances –
          for example, a test that expects a virtual-time timeout to cancel
          and abandon a stuck ``abandon_on_cancel=True`` thread – then
          waiting for it will hang. Set this attribute to False to restore
          the old behavior of jumping while threads run, or advance the
          clock explicitly with :meth:`jump`.

    """

    def __init__(
        self,
        rate: float = 0.0,
        autojump_threshold: float = inf,
        autojump_wait_for_threads: bool = True,
    ) -> None:
        # when the real clock said 'real_base', the virtual time was
        # 'virtual_base', and since then it's advanced at 'rate' virtual
        # seconds per real second.
        self._real_base = 0.0
        self._virtual_base = 0.0
        self._rate = 0.0

        # kept as an attribute so that our tests can monkeypatch it
        self._real_clock = time.perf_counter

        # plain defaults first, so the property setters (which resync both
        # values to the runner) never see a half-initialized clock
        self._autojump_threshold = inf
        self._autojump_wait_for_threads = True

        # use the property update logic to set initial values
        self.rate = rate
        self.autojump_threshold = autojump_threshold
        self.autojump_wait_for_threads = autojump_wait_for_threads

    def __repr__(self) -> str:
        return f"<MockClock, time={self.current_time():.7f}, rate={self._rate} @ {id(self):#x}>"

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, new_rate: float) -> None:
        if new_rate < 0:
            raise ValueError("rate must be >= 0")
        else:
            real = self._real_clock()
            virtual = self._real_to_virtual(real)
            self._virtual_base = virtual
            self._real_base = real
            self._rate = float(new_rate)

    @property
    def autojump_threshold(self) -> float:
        return self._autojump_threshold

    @autojump_threshold.setter
    def autojump_threshold(self, new_autojump_threshold: float) -> None:
        self._autojump_threshold = float(new_autojump_threshold)
        self._try_resync_autojump_threshold()

    @property
    def autojump_wait_for_threads(self) -> bool:
        return self._autojump_wait_for_threads

    @autojump_wait_for_threads.setter
    def autojump_wait_for_threads(self, new_value: bool) -> None:
        self._autojump_wait_for_threads = bool(new_value)
        self._try_resync_autojump_threshold()

    # runner.clock_autojump_threshold is an internal API that isn't easily
    # usable by custom third-party Clock objects. If you need access to this
    # functionality, let us know, and we'll figure out how to make a public
    # API. Discussion:
    #
    #     https://github.com/python-trio/trio/issues/1587
    def _try_resync_autojump_threshold(self) -> None:
        try:
            runner = GLOBAL_RUN_CONTEXT.runner
            if runner.is_guest:
                runner.force_guest_tick_asap()
        except AttributeError:
            pass
        else:
            if runner.clock is self:
                runner.clock_autojump_threshold = self._autojump_threshold
                runner.clock_autojump_wait_for_holds = self._autojump_wait_for_threads

    # Invoked by the run loop when runner.clock_autojump_threshold is
    # exceeded.
    def _autojump(self) -> None:
        statistics = _core.current_statistics()
        jump = statistics.seconds_to_next_deadline
        if 0 < jump < inf:
            self.jump(jump)

    def _real_to_virtual(self, real: float) -> float:
        real_offset = real - self._real_base
        virtual_offset = self._rate * real_offset
        return self._virtual_base + virtual_offset

    def start_clock(self) -> None:
        self._try_resync_autojump_threshold()

    def current_time(self) -> float:
        return self._real_to_virtual(self._real_clock())

    def deadline_to_sleep_time(self, deadline: float) -> float:
        virtual_timeout = deadline - self.current_time()
        if virtual_timeout <= 0:
            return 0
        elif self._rate > 0:
            return virtual_timeout / self._rate
        else:
            return 999999999

    def jump(self, seconds: float) -> None:
        """Manually advance the clock by the given number of seconds.

        Args:
          seconds (float): the number of seconds to jump the clock forward.

        Raises:
          ValueError: if you try to pass a negative value for ``seconds``.

        """
        if seconds < 0:
            raise ValueError("time can't go backwards")
        self._virtual_base += seconds
