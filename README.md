# sociedad-sitemap

Generador de sitemaps para [sociedad.info](sociedad.info)

## Uso

```
node index.js -u [Elasticsearch URL] -b [URL base] -l [ubicación]
```

### Parámetros

```
-u      --elasticUri    URL de Elasticsearch
-b      --baseUrl       URL base para todos los URLs (https://sociedad.info)
-l      --location      Segmento de URL de los sitemaps (sitemaps -> /static/sitemaps/)
-c      --countries     Archivo countries.json (código de país -> slug)
-d      --country       Solo un país (código de dos letras)
-n      --limit         Solo para pruebas: corta cada índice después de N documentos
-t      --test          No escribe archivos
```

## Salida

Genera archivos en la carpeta *sitemaps* con: 
- Sitemaps de 50,000 elementos cada uno para proveedores, contratos y entidades
- Sitemap con páginas estáticas
- Archivo índice de todos los sitemaps

Escribe en `./sitemaps.tmp` y al terminar la reemplaza por `./sitemaps` (relativo al directorio de trabajo). Si algo falla sale con código 1 y deja los sitemaps publicados como estaban.

No lista a las personas físicas de GT sin adjudicaciones que no representan a ninguna empresa, que en el sitio llevan `noindex`. Ver `docs/2026-06-24-pending-seo-fixes.md`.
