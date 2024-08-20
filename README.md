# sociedad-sitemap

Generador de sitemaps para [sociedad.info](sociedad.info)

## Uso

```
node index.js -u [Elasticsearch URL] -b [URL base]
```

### Parámetros

```
-u      --elasticUri    URL de Elasticsearch
-b      --baseUrl       URL base para todos los URLs (https://sociedad.info)
```

## Salida

Genera archivos en la carpeta *sitemaps* con: 
- Sitemaps de 50,000 elementos cada uno para proveedores, contratos y entidades
- Sitemap con páginas estáticas
- Archivo índice de todos los sitemaps
