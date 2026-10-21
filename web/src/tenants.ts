// The tenant registry.
//
// The kernel scopes every resource by the tenant of the authenticated
// principal. An operator who administers several tenants holds one token per
// tenant; the registry keeps them apart and hands the console the token of
// the tenant that is in focus.
//
// The tokens live in the session storage of the browser: they do not survive
// the session and they are never written to the server except as a header.

export interface Tenant {
  id: string;
  token: string;
  label?: string;
}

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const STORAGE_KEY = 'fenced.operator.tenants';

export class TenantRegistry {
  private tenants: Tenant[] = [];
  private activeId = '';

  constructor(private readonly storage?: StorageLike) {
    this.load();
  }

  list(): Tenant[] {
    return [...this.tenants];
  }

  active(): Tenant | undefined {
    return this.tenants.find((tenant) => tenant.id === this.activeId);
  }

  activeId_(): string {
    return this.activeId;
  }

  get(id: string): Tenant | undefined {
    return this.tenants.find((tenant) => tenant.id === id);
  }

  tokenFor(id: string): string | undefined {
    return this.get(id)?.token;
  }

  add(tenant: Tenant): void {
    const existing = this.tenants.findIndex((candidate) => candidate.id === tenant.id);
    if (existing >= 0) {
      this.tenants[existing] = tenant;
    } else {
      this.tenants.push(tenant);
    }
    if (!this.activeId) {
      this.activeId = tenant.id;
    }
    this.save();
  }

  remove(id: string): void {
    this.tenants = this.tenants.filter((tenant) => tenant.id !== id);
    if (this.activeId === id) {
      this.activeId = this.tenants[0]?.id ?? '';
    }
    this.save();
  }

  setActive(id: string): void {
    if (!this.get(id)) {
      throw new Error(`unknown tenant ${id}`);
    }
    this.activeId = id;
    this.save();
  }

  /** The tenants that carry a token; a tenant without one cannot be read. */
  readable(): Tenant[] {
    return this.tenants.filter((tenant) => tenant.token.trim() !== '');
  }

  save(): void {
    if (!this.storage) {
      return;
    }
    this.storage.setItem(STORAGE_KEY, JSON.stringify({ activeId: this.activeId, tenants: this.tenants }));
  }

  load(): void {
    if (!this.storage) {
      return;
    }
    const raw = this.storage.getItem(STORAGE_KEY);
    if (!raw) {
      return;
    }
    try {
      const parsed = JSON.parse(raw) as { activeId?: string; tenants?: Tenant[] };
      const tenants = Array.isArray(parsed.tenants) ? parsed.tenants : [];
      this.tenants = tenants.filter(
        (tenant): tenant is Tenant =>
          typeof tenant?.id === 'string' && typeof tenant?.token === 'string',
      );
      this.activeId =
        typeof parsed.activeId === 'string' && this.get(parsed.activeId)
          ? parsed.activeId
          : (this.tenants[0]?.id ?? '');
    } catch {
      this.tenants = [];
      this.activeId = '';
    }
  }

  clear(): void {
    this.tenants = [];
    this.activeId = '';
    if (this.storage) {
      this.storage.removeItem(STORAGE_KEY);
    }
  }
}

/** A storage that keeps nothing; the default when there is no browser. */
export class MemoryStorage implements StorageLike {
  private readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}
