// The console shell: the tenants it may read, the view in focus, the theme
// and the language.

import { useCallback, useMemo, useState, type ReactNode } from 'react';
import { ControlClient } from './api';
import { TenantRegistry, type StorageLike } from './tenants';
import { translator, type Language } from './i18n';
import { Badge, Chip, Notice } from './ui';
import { AuditView, FleetView, GovernanceView, ServicesView, TasksView } from './views';

type View = 'fleet' | 'tasks' | 'services' | 'governance' | 'audit';

const VIEWS: View[] = ['fleet', 'tasks', 'services', 'governance', 'audit'];

export function App(props: { storage?: StorageLike }): ReactNode {
  const registry = useMemo(() => new TenantRegistry(props.storage), [props.storage]);
  const [tenantIds, setTenantIds] = useState<string[]>(() => registry.list().map((t) => t.id));
  const [activeId, setActiveId] = useState<string>(() => registry.activeId_());
  const [view, setView] = useState<View>('fleet');
  const [language, setLanguage] = useState<Language>('en');
  const [theme, setTheme] = useState('dark');
  const [version, setVersion] = useState('');
  const [notice, setNotice] = useState('');

  const t = useMemo(() => translator(language), [language]);
  const tokens = useMemo(() => {
    const map = new Map<string, string>();
    for (const tenant of registry.list()) {
      map.set(tenant.id, tenant.token);
    }
    return map;
  }, [registry, tenantIds]);

  const clientFor = useCallback(
    (tenantId: string): ControlClient => {
      const token = tokens.get(tenantId);
      if (token === undefined) {
        throw new Error(`no token for tenant ${tenantId}`);
      }
      return new ControlClient({ token });
    },
    [tokens],
  );

  const activeClient = useMemo(() => {
    const token = tokens.get(activeId);
    return token ? new ControlClient({ token }) : undefined;
  }, [tokens, activeId]);

  // The product version comes from the control plane itself.
  useMemo(() => {
    if (!activeClient) {
      return undefined;
    }
    activeClient
      .version()
      .then((info) => setVersion(`${info.product ?? 'Fenced'} ${info.productVersion ?? ''}`.trim()))
      .catch(() => setVersion(''));
    return undefined;
  }, [activeClient]);

  const refreshTenants = useCallback(() => {
    setTenantIds(registry.list().map((tenant) => tenant.id));
    setActiveId(registry.activeId_());
  }, [registry]);

  const addTenant = () => {
    const id = window.prompt(t('tenantId'));
    if (!id) {
      return;
    }
    const token = window.prompt(t('token'));
    if (!token) {
      return;
    }
    registry.add({ id: id.trim(), token: token.trim() });
    refreshTenants();
    registry.setActive(id.trim());
    setActiveId(id.trim());
    setNotice(`${t('connect')}: ${id.trim()}`);
  };

  const removeTenant = (id: string) => {
    registry.remove(id);
    refreshTenants();
  };

  const focusTenant = (id: string) => {
    registry.setActive(id);
    setActiveId(id);
  };

  return (
    <div className={`shell theme-${theme}`}>
      <header className="topbar">
        <div className="brand">
          <span className="brand-name">{t('product')}</span>
          <span className="brand-sub">{t('console')}</span>
          {version && <Badge>{version}</Badge>}
        </div>

        <div className="topbar-controls">
          {tenantIds.length > 0 && (
            <select
              value={activeId}
              onChange={(event) => focusTenant(event.target.value)}
              aria-label={t('tenants')}
            >
              {tenantIds.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          )}
          <Chip onClick={addTenant}>{t('addTenant')}</Chip>
          <Chip onClick={() => setLanguage(language === 'en' ? 'zh' : 'en')}>EN / 中文</Chip>
          <Chip onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>◐</Chip>
        </div>
      </header>

      <div className="body">
        <nav className="sidebar">
          {VIEWS.map((name) => (
            <button
              key={name}
              type="button"
              className={name === view ? 'nav-item active' : 'nav-item'}
              onClick={() => setView(name)}
            >
              {t(name)}
            </button>
          ))}
          {tenantIds.length > 1 && (
            <div className="tenant-list">
              <span className="nav-title">{t('tenants')}</span>
              {tenantIds.map((id) => (
                <div key={id} className="tenant-row">
                  <button type="button" className="tenant-id" onClick={() => focusTenant(id)}>
                    {id}
                  </button>
                  <button type="button" className="tenant-remove" onClick={() => removeTenant(id)}>
                    ×
                  </button>
                </div>
              ))}
            </div>
          )}
        </nav>

        <main className="content">
          {notice && <Notice>{notice}</Notice>}
          {view === 'fleet' && (
            <FleetView
              tenantIds={tenantIds}
              labelFor={(id) => registry.get(id)?.label}
              clientFor={clientFor}
              onFocus={focusTenant}
              t={t}
              language={language}
            />
          )}
          {view !== 'fleet' && !activeClient && <Notice>{t('noTenant')}</Notice>}
          {view === 'tasks' && activeClient && <TasksView client={activeClient} t={t} language={language} />}
          {view === 'services' && activeClient && (
            <ServicesView client={activeClient} t={t} language={language} />
          )}
          {view === 'governance' && activeClient && (
            <GovernanceView client={activeClient} t={t} language={language} />
          )}
          {view === 'audit' && activeClient && <AuditView client={activeClient} t={t} language={language} />}
        </main>
      </div>
    </div>
  );
}
