export default class AsyncMutex {
  private locked = false;
  private readonly waiters: Array<() => void> = [];

  async runExclusive<T>(callback: () => T | Promise<T>): Promise<T> {
    await this.acquire();

    try {
      return await callback();
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (!this.locked) {
      this.locked = true;
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private release(): void {
    const next = this.waiters.shift();

    if (next) {
      next();
      return;
    }

    this.locked = false;
  }
}
