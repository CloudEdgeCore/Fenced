import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './app';
import './styles.css';

const container = document.getElementById('root');
if (!container) {
  throw new Error('the console document has no root element');
}

createRoot(container).render(
  <StrictMode>
    <App storage={window.sessionStorage} />
  </StrictMode>,
);
