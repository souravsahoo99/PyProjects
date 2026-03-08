import asyncio
import time


class RestLTPPoller:
    """
    Polls LTP using SimpleShoonyaEngine REST interface.
    Designed as orchestration layer only.
    """

    def __init__(self, engine, exchange: str, token: str,
                 interval: float = 1.0):
        """
        engine   : SimpleShoonyaEngine instance
        exchange : Exchange name (e.g., 'NSE')
        token    : Instrument token
        interval : Poll frequency in seconds
        """

        self.engine = engine
        self.exchange = exchange
        self.token = token
        self.interval = interval

        self._latest_price = None
        self._is_running = False
        self._task = None

    # -------------------------
    # INTERNAL LOOP
    # -------------------------

    async def _poll_loop(self):

        while self._is_running:

            cycle_start = time.time()

            try:
                price = self.engine.get_ltp_rest(
                    self.exchange,
                    self.token
                )
                self._latest_price = price

            except Exception:
                # Fail silently — wrapper1 handles retry
                pass

            elapsed = time.time() - cycle_start
            sleep_duration = max(0, self.interval - elapsed)

            await asyncio.sleep(sleep_duration)

    # -------------------------
    # CONTROL METHODS
    # -------------------------

    async def start(self):

        if self._is_running:
            return

        self._is_running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self):

        self._is_running = False

        if self._task:
            await self._task

    # -------------------------
    # DATA ACCESS
    # -------------------------

    def get_latest(self):
        return self._latest_price