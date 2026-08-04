import threading
import time
from math import inf

import pytest

from trio import sleep

from ... import _core
from .. import wait_all_tasks_blocked
from .._mock_clock import MockClock
from .._run import GLOBAL_RUN_CONTEXT
from .tutil import slow


def test_mock_clock() -> None:
    REAL_NOW = 123.0
    c = MockClock()
    c._real_clock = lambda: REAL_NOW
    repr(c)  # smoke test
    assert c.rate == 0
    assert c.current_time() == 0
    c.jump(1.2)
    assert c.current_time() == 1.2
    with pytest.raises(ValueError, match=r"^time can't go backwards$"):
        c.jump(-1)
    assert c.current_time() == 1.2
    assert c.deadline_to_sleep_time(1.1) == 0
    assert c.deadline_to_sleep_time(1.2) == 0
    assert c.deadline_to_sleep_time(1.3) > 999999

    with pytest.raises(ValueError, match=r"^rate must be >= 0$"):
        c.rate = -1
    assert c.rate == 0

    c.rate = 2
    assert c.current_time() == 1.2
    REAL_NOW += 1
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1) == 0
    assert c.deadline_to_sleep_time(3.2) == 0
    assert c.deadline_to_sleep_time(4.2) == 0.5

    c.rate = 0.5
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1) == 0
    assert c.deadline_to_sleep_time(3.2) == 0
    assert c.deadline_to_sleep_time(4.2) == 2.0

    c.jump(0.8)
    assert c.current_time() == 4.0
    REAL_NOW += 1
    assert c.current_time() == 4.5

    c2 = MockClock(rate=3)
    assert c2.rate == 3
    assert c2.current_time() < 10


async def test_mock_clock_autojump(mock_clock: MockClock) -> None:
    assert mock_clock.autojump_threshold == inf

    mock_clock.autojump_threshold = 0
    assert mock_clock.autojump_threshold == 0

    real_start = time.perf_counter()

    virtual_start = _core.current_time()
    for i in range(10):
        print(f"sleeping {10 * i} seconds")
        await sleep(10 * i)
        print("woke up!")
        assert virtual_start + 10 * i == _core.current_time()
        virtual_start = _core.current_time()

    real_duration = time.perf_counter() - real_start
    print(f"Slept {10 * sum(range(10))} seconds in {real_duration} seconds")
    assert real_duration < 1

    mock_clock.autojump_threshold = 0.02
    t = _core.current_time()
    # this should wake up before the autojump threshold triggers, so time
    # shouldn't change
    await wait_all_tasks_blocked()
    assert t == _core.current_time()
    # this should too
    await wait_all_tasks_blocked(0.01)
    assert t == _core.current_time()

    # set up a situation where the autojump task is blocked for a long long
    # time, to make sure that cancel-and-adjust-threshold logic is working
    mock_clock.autojump_threshold = 10000
    await wait_all_tasks_blocked()
    mock_clock.autojump_threshold = 0
    # if the above line didn't take affect immediately, then this would be
    # bad:
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


async def test_mock_clock_autojump_interference(mock_clock: MockClock) -> None:
    mock_clock.autojump_threshold = 0.02

    mock_clock2 = MockClock()
    # messing with the autojump threshold of a clock that isn't actually
    # installed in the run loop shouldn't do anything.
    mock_clock2.autojump_threshold = 0.01

    # if the autojump_threshold of 0.01 were in effect, then the next line
    # would block forever, as the autojump task kept waking up to try to
    # jump the clock.
    await wait_all_tasks_blocked(0.015)

    # but the 0.02 limit does apply
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


def test_mock_clock_autojump_preset() -> None:
    # Check that we can set the autojump_threshold before the clock is
    # actually in use, and it gets picked up
    mock_clock = MockClock(autojump_threshold=0.1)
    mock_clock.autojump_threshold = 0.01
    real_start = time.perf_counter()
    _core.run(sleep, 10000, clock=mock_clock)
    assert time.perf_counter() - real_start < 1


async def test_mock_clock_autojump_0_and_wait_all_tasks_blocked_0(
    mock_clock: MockClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with the default cushion=0.

    mock_clock.autojump_threshold = 0

    record = []

    async def sleeper() -> None:
        await sleep(100)
        record.append("yawn")

    async def waiter() -> None:
        await wait_all_tasks_blocked()
        record.append("waiter woke")
        await sleep(1000)
        record.append("waiter done")

    async with _core.open_nursery() as nursery:
        nursery.start_soon(sleeper)
        nursery.start_soon(waiter)

    assert record == ["waiter woke", "yawn", "waiter done"]


@slow
async def test_mock_clock_autojump_0_and_wait_all_tasks_blocked_nonzero(
    mock_clock: MockClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with a non-zero cushion.

    mock_clock.autojump_threshold = 0

    record = []

    async def sleeper() -> None:
        await sleep(100)
        record.append("yawn")

    async def waiter() -> None:
        await wait_all_tasks_blocked(1)
        record.append("waiter done")

    async with _core.open_nursery() as nursery:
        nursery.start_soon(sleeper)
        nursery.start_soon(waiter)

    assert record == ["waiter done", "yawn"]


async def test_initialization_doesnt_mutate_runner() -> None:
    before = (
        GLOBAL_RUN_CONTEXT.runner.clock,
        GLOBAL_RUN_CONTEXT.runner.clock_autojump_threshold,
        GLOBAL_RUN_CONTEXT.runner.clock_autojump_wait_for_holds,
    )

    MockClock(autojump_threshold=2, rate=3, autojump_wait_for_threads=False)

    after = (
        GLOBAL_RUN_CONTEXT.runner.clock,
        GLOBAL_RUN_CONTEXT.runner.clock_autojump_threshold,
        GLOBAL_RUN_CONTEXT.runner.clock_autojump_wait_for_holds,
    )
    assert before == after


async def test_autojump_hold_api() -> None:
    hold = _core.autojump_hold(name="test hold")
    repr(hold)  # smoke test
    assert hold.name == "test hold"
    assert not hold.released
    assert _core.current_statistics().autojump_holds == ("test hold",)

    hold.release()
    assert hold.released
    assert _core.current_statistics().autojump_holds == ()
    with pytest.raises(RuntimeError, match="already been released"):
        hold.release()

    # holds can also be used as context managers
    with _core.autojump_hold(name="cm hold") as entered:
        # __enter__ returns None: the hold is active from the moment
        # autojump_hold() returns, and the with-block is its lifetime
        assert entered is None
        assert _core.current_statistics().autojump_holds == ("cm hold",)
    assert _core.current_statistics().autojump_holds == ()

    # __exit__ tolerates an early manual release...
    hold2 = _core.autojump_hold(name="early release")
    with hold2:
        hold2.release()
    # ...but re-entering a released hold is an error
    with pytest.raises(RuntimeError, match="re-enter"):
        with hold2:
            pass  # pragma: no cover

    # multiple holds are reported sorted by name
    a = _core.autojump_hold(name="a")
    b = _core.autojump_hold(name="b")
    assert _core.current_statistics().autojump_holds == ("a", "b")
    a.release()
    b.release()

    # the default name
    with _core.autojump_hold():
        assert _core.current_statistics().autojump_holds == ("<unnamed>",)


def test_autojump_hold_outside_run() -> None:
    with pytest.raises(RuntimeError, match="must be called from async context"):
        _core.autojump_hold()


def test_autojump_hold_no_public_constructor() -> None:
    with pytest.raises(TypeError):
        _core.AutojumpHold()  # type: ignore[call-arg]


async def test_autojump_waits_for_holds(mock_clock: MockClock) -> None:
    # While a hold is active, the clock must not autojump; it should jump
    # promptly once the hold is released (here, from another thread via
    # run_sync_soon, the way external work reports back).
    mock_clock.autojump_threshold = 0
    assert mock_clock.autojump_wait_for_threads

    hold = _core.autojump_hold(name="external work")
    token = _core.current_trio_token()
    released = False

    def do_release() -> None:
        nonlocal released
        released = True
        hold.release()

    def releaser() -> None:
        time.sleep(0.2)
        token.run_sync_soon(do_release)

    thread = threading.Thread(target=releaser)
    thread.start()
    try:
        virtual_start = _core.current_time()
        # This can only complete via autojump, and autojump can only happen
        # after do_release runs.
        await sleep(100)
        assert released
        assert _core.current_time() == virtual_start + 100
    finally:
        thread.join()


async def test_autojump_holds_ignored_when_wait_disabled(
    mock_clock: MockClock,
) -> None:
    mock_clock.autojump_threshold = 0
    mock_clock.autojump_wait_for_threads = False
    assert not mock_clock.autojump_wait_for_threads

    with _core.autojump_hold(name="ignored"):
        # With autojump_wait_for_threads disabled, the clock jumps right
        # through active holds, restoring the old behavior. (If the hold were
        # respected, this would hang.)
        await sleep(100)
