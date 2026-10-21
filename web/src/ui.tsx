// The shared building blocks of the console. The design system lives in
// styles.css; these components only give it structure.

import type { ReactNode } from 'react';

export function Card(props: {
  title?: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  accent?: string;
}): ReactNode {
  return (
    <section className="card" style={props.accent ? { borderTop: `2px solid ${props.accent}` } : undefined}>
      {(props.title || props.actions) && (
        <header className="card-head">
          <div>
            {props.title && <h2 className="card-title">{props.title}</h2>}
            {props.subtitle && <p className="card-sub">{props.subtitle}</p>}
          </div>
          {props.actions && <div className="card-actions">{props.actions}</div>}
        </header>
      )}
      {props.children}
    </section>
  );
}

export function Chip(props: {
  children: ReactNode;
  onClick?: () => void;
  tone?: 'primary' | 'danger' | 'muted' | undefined;
  title?: string | undefined;
}): ReactNode {
  const className = props.tone && props.tone !== 'primary' ? `chip chip-${props.tone}` : 'chip';
  return (
    <button type="button" className={className} onClick={props.onClick} title={props.title}>
      {props.children}
    </button>
  );
}

export function Badge(props: { children: ReactNode; tone?: string | undefined }): ReactNode {
  return <span className={props.tone ? `badge badge-${props.tone}` : 'badge'}>{props.children}</span>;
}

export function Table(props: {
  headers: string[];
  children: ReactNode;
  empty?: ReactNode;
}): ReactNode {
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            {props.headers.map((header) => (
              <th key={header}>{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>{props.children}</tbody>
      </table>
      {props.empty}
    </div>
  );
}

export function Empty(props: { text: string }): ReactNode {
  return (
    <div className="empty">
      <span>{props.text}</span>
    </div>
  );
}

export function ErrorBanner(props: { message: string; onDismiss?: () => void }): ReactNode {
  return (
    <div className="banner banner-error" role="alert">
      <span>{props.message}</span>
      {props.onDismiss && (
        <button type="button" className="banner-close" onClick={props.onDismiss}>
          ×
        </button>
      )}
    </div>
  );
}

export function Notice(props: { children: ReactNode }): ReactNode {
  return <div className="banner banner-notice">{props.children}</div>;
}

export function Field(props: { label: string; children: ReactNode }): ReactNode {
  return (
    <label className="field">
      <span className="field-label">{props.label}</span>
      {props.children}
    </label>
  );
}

export function when(value: string | undefined | null): string {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString().replace('T', ' ').slice(0, 19);
}
