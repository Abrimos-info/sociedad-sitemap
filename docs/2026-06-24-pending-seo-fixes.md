# Pending SEO fixes — sociedad-sitemap

> **Actualizado 2026-11-09.** Los dos fixes de abajo ya estaban resueltos en la v2 (`8fa3a4c`): las URLs de proveedor usan `/supplier/` y los compradores salen de `sociedad_buyers` por `id`. Lo que sigue pendiente o se cambió después está en la sección final.

**Date:** 2026-06-24  
**Context:** Derived from SEO audit `sociedad-web-front/docs/2026-06-19-seo-audit.md` (F1, F2).  
These changes were drafted on the GitLab repo and reverted before migrating to GitHub.  
Apply to the GitHub version after reviewing conflicts with newer code.

---

## Fix 1 — Supplier sitemap: legacy `/proveedor/` URL (F1) — resuelto en v2

**File:** `index.js`  
**Change:** One line in `run()`:

```js
// Before
args.baseUrl + '/guatemala/proveedor/'
// After
args.baseUrl + '/guatemala/supplier/'
```

**Why:** Sitemaps must use canonical URLs. `/proveedor/` 301-redirects to `/supplier/`. 8.7M redirect hops waste crawl budget and send conflicting canonical signals.

---

## Fix 2 — Buyer sitemap: use buyer IDs from `sociedad_buyers` (F2) — resuelto en v2

**File:** `index.js`  
**What the GitLab draft did (incomplete):**  
- Replaced the `entidad_compradora.keyword` aggregation (name-based, fragile) with a `buyer.id.keyword` aggregation on `guatecompras_contratos`  
- Changed URL from `/guatemala/entidad/{name}` to `/guatemala/buyer/{id}`  
- Removed UC sub-aggregation  

**Why the draft is still wrong:**  
`buyer.id.keyword` on `guatecompras_contratos` returns empty buckets — the field doesn't exist there.  
The correct source is the `sociedad_buyers` index (production alias: `sociedad_buyers`, dev: `beta_sociedad_buyers`).

**Correct fix to implement:**

```js
// New constant
const buyersIndex = 'sociedad_buyers';

// New query — scroll, not aggregation
const query_buyer = {
    "query": {
        "term": { "country.keyword": "GT" }  // GT only, consistent with supplier/contract scope
    }
}
// Call — no aggs flag (scroll mode)
sitemaps.push(...await buildSitemaps(buyersIndex, 'buyer', query_buyer, 'id', 'updated_date', args.baseUrl + '/guatemala/buyer/'));
```

**Also needed:** Add `buyer` case to `determineChangefreq` — currently falls through to `undefined`:

```js
case 'buyer':
    return 'monthly';
```

**Field reference (confirmed against live index):**
- Index: `sociedad_buyers` (4,718 GT documents)
- ID field: `id` (slug, e.g. `ministerio-de-salud-publica`)
- Date field: `updated_date` (ISO8601)
- Country field: `country` (2-letter code, e.g. `GT`)

**Note on scope:** GT-only filter is consistent with supplier and contract sitemaps, which also cover only Guatemala. Multi-country buyer sitemap is a separate future expansion.


---

## 2026-11-09 — cambios en la branch `seo/2026-11-indices-personas-uc`

Contexto: `sociedad-web-front/docs/2026-11-09-pendientes-seo-e-infra.md`, punto S2.

- **Índices.** `sociedad_buyers` y `sociedad_suppliers` ya no existen (hay un alias por país desde junio). La corrida diaria de NiFi fallaba con `index_not_found_exception` y los sitemaps quedaron congelados el 2026-08-03. Ahora usa `sociedad_buyers_*` y `sociedad_suppliers_*`, igual que el front.
- **Publicación atómica.** Escribe en `sitemaps.tmp` y reemplaza `sitemaps` al final. Si falla, sale con código 1 y deja publicados los anteriores. Si no generó sitemaps de compradores o de proveedores, falla. Los archivos que dejan de generarse desaparecen.
- **`lastmod` del índice.** Cada sitemap hijo conserva el `lastmod` anterior si su contenido no cambió.
- **Personas sin actividad.** No se listan las personas de GT sin `has_awards` que no aparecen como `representatives.id` en ninguna ficha. Es la misma regla que `sociedad-web-front/lib/person-indexing.js`, que les pone `noindex,follow`. Si cambia en un lado, cambiarla en el otro.
- **Unidades compradoras.** Las `buyer_unit` se listan como `/buyer/{member_of.id}/buyer-unit/{id}`. `/buyer/{id}` responde 301 a esa URL desde el front.
- **`--limit` / `-n`.** Solo para pruebas: corta cada índice después de N documentos.

Sigue pendiente, por decisión:

- Contratos: no están en ningún sitemap desde la v2.
- Países inactivos: se mantienen (21.753 URLs de 11,9 millones).
- IndexNow y `lastmod` de proveedor desde el último contrato: descartados por ahora.
