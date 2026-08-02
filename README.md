# TradingView EPS Service

Servicio Node.js/Express que abre el WebSocket de TradingView y devuelve
`eps_estimates_fy_h` como JSON.

## Desarrollo local

```bash
npm install
npm start
```

Prueba:

```text
http://localhost:10000/eps?symbol=NASDAQ:NVDA
```

## Variables de entorno

- `PORT`: la asigna normalmente el proveedor.
- `API_KEY`: opcional. Si se configura, las llamadas a `/eps` deben incluir
  `x-api-key: TU_CLAVE` o `Authorization: Bearer TU_CLAVE`.

## Render

- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check path: `/health`
