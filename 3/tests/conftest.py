class DummyChClient:
    def __init__(self):
        self.executed = []
        self.closed = False

    async def execute(self, *args, **kwargs):  # pragma: no cover - safety
        self.executed.append((args, kwargs))

    async def close(self):
        self.closed = True