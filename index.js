const fs = require('fs');
const { Client } = require("@opensearch-project/opensearch");
const { encodeSitemapURL } = require('node-uri');
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
    { name: 'dbUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'baseUrl', alias: 'b', type: String },
    { name: 'location', alias: 'l', type: String },
    { name: 'test', alias: 't', type: Boolean, defaultValue: false }
];
const args = commandLineArgs(optionDefinitions);
if(!args.dbUri || !args.baseUrl) {
    console.error("ERROR: missing params.");
    process.exit(1);
}

const today = new Date();
const dbNode = args.dbUri;
let client = getClient(dbNode);
const batchSize = 10000;
const scrollTimeout = '600s';
let query = {
    "query": {
        "match_all": {}
    }
}
let sitemaps = [];
const proveedoresIndex = 'guatecompras_proveedores';
const contratosIndex = 'guatecompras_contratos';
const sitemapItemCount = 25000;
let proveedores_cache = {}

run();

async function run() {
    console.log('Starting')
    
    // console.log('Generating static sitemap...');
    // buildStaticSitemap(args.baseUrl);
    // sitemaps.push('sitemap_static.xml');

    console.log('Getting proveedores cache...')
    proveedores_cache = await getProveedoresCache();
    
    console.log('Getting proveedores...')
    sitemaps.push(...await buildSitemaps(proveedoresIndex, 'proveedor', query, 'nit', 'fecha_sat', args.baseUrl + '/guatemala/proveedor/'));
    
    console.log('Getting entidades...')
    query_entidad = {
        "size": 0, 
        "aggs": {
            "name": {
                "terms": {
                    "size": 25000,
                    "field": "entidad_compradora.keyword" //equivale a dependencia, es el dato más importante
                },
                "aggs": {
                    "lastmod": {
                        "max": {
                            "field": "fecha_publicacion", 
                            "format": "yyyy-MM-dd'T'HH:mm:sszzz"
                        }
                    },
                    "uc": {
                        "terms": {
                            "size": 1000,
                            "field": "unidad_compradora.keyword"
                        },
                        "aggs": {
                            "lastmod": {
                                "max": {
                                    "field": "fecha_publicacion",
                                    "format": "yyyy-MM-dd'T'HH:mm:sszzz"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    sitemaps.push(...await buildSitemaps(contratosIndex, 'entidad', query_entidad, 'name', 'lastmod', args.baseUrl + '/guatemala/entidad/', true)); 

    console.log('Getting contracts...')
    sitemaps.push(...await buildSitemaps(contratosIndex, 'contract', query, 'nog_concurso', 'fecha_publicacion', args.baseUrl + '/guatemala/contract/'));

    console.log('Generating sitemap index...');
    buildSitemapIndex(sitemaps, args.baseUrl, args.location);
    console.log('Finished');
}


function getClient(elasticNode) {
    let client = null;
    try {
        client = new Client({ node: elasticNode, requestTimeout: 60000, maxRetries: 10, sniffOnStart: false, ssl: { rejectUnauthorized: false }, resurrectStrategy: "none", compression: "gzip" })
    }
    catch (e) {
        console.error("getClient",e);
    }
    return client;
}

function buildSitemapIndex(filenames, base, location) {
    let uris = [];
    uris.push({uri: 'https://sociedad.info/sitemap-static.xml', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")} );
    filenames.map( file => {
        uris.push({uri:  base + '/static/' + location + '/' + file, lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")} );
    } );

    writeSitemap(uris, 'index', 0, true);
}

function buildStaticSitemap(base) {
    let uris = [];
    
    uris.push({uri: base + '/', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/buscador', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/acerca-de', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/contacto', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/privacidad', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});

    writeSitemap(uris, 'static');
}

async function buildSitemaps(index, type, docQuery, idField, lastModField, location, aggs=false) {
    let allDocs = 0;
    let sitemapFiles = [];
    let sitemapCount = 0;
    let uriBuffer = [];
    const responseQueue = []

    let options = { body: {} }
    if(!aggs) {
        options = {
            "index": index,
            "scroll": scrollTimeout,
            "size": batchSize,
            "_source": false,
            "body": {
                "fields": [ idField, lastModField ]
            }
        };
        Object.assign(options.body, docQuery);
    }
    Object.assign(options.body, docQuery);
    const response = await client.search(options)

    responseQueue.push(response)
    let id, lastmod, changefreq = null;

    if(aggs) {
        const data = responseQueue.shift();
        let buckets = data.body.aggregations[idField].buckets;
        if(buckets.length > 0) {
            buckets.map(b => {
                allDocs++;
                id = b.key;
                lastmod = b.lastmod.value_as_string;
                changefreq = determineChangefreq(type, lastmod);
                uriBuffer.push({uri: encodeSitemapURL(location + id), lastmod: lastmod, changefreq: changefreq});

                if(b.uc?.buckets?.length > 0) {
                    let ucs = b.uc.buckets;
                    ucs.map( uc => {
                        allDocs++;
                        let uc_id = uc.key;
                        let uc_lastmod = uc.lastmod.value_as_string;
                        let uc_changefreq = determineChangefreq(type, uc_lastmod);
                        uriBuffer.push({uri: encodeSitemapURL(location + id + '/unidad-compradora/' + uc_id), lastmod: uc_lastmod, changefreq: uc_changefreq});
                    } )
                }

                if(allDocs % sitemapItemCount == 0) {
                    sitemapCount++;
                    sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
                    uriBuffer = [];
                }
            });

            sitemapCount++;
            sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
            uriBuffer = [];
        }
    }
    else {
        while (responseQueue.length) {
            const response = responseQueue.shift()
            // collect the docs from this response
            for(let i=0; i<response.body.hits.hits.length; i++) {
                let hit = response.body.hits.hits[i];
                let id = '';
                if(hit.hasOwnProperty('fields') && hit.fields.hasOwnProperty(idField)) {
                    id = hit.fields[idField][0];
                    
                    if(type == 'proveedor') {
                        if(proveedores_cache.hasOwnProperty(id))
                            lastmod = proveedores_cache[id];
                        else if (hit.fields[lastModField]) 
                            lastmod = hit.fields[lastModField][0];
                        else lastmod = new Date('2025-05-22').toISOString();
                    }
                    else if (hit.fields[lastModField]) 
                        lastmod = hit.fields[lastModField][0];
                    else 
                        lastmod = null;

                    changefreq = determineChangefreq(type, lastmod);
                    uriBuffer.push({uri: encodeSitemapURL(location + id), lastmod: lastmod, changefreq: changefreq});
                }
                allDocs++;
                if(allDocs % sitemapItemCount == 0) {
                    sitemapCount++;
                    sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
                    uriBuffer = [];
                }
            }
    
            // check to see if we have collected all docs
            if(response.body.hits.total.value <= allDocs) {
                sitemapCount++;
                sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
                uriBuffer = [];
                break;
            }
    
            // get the next response if there are more docs to fetch
            responseQueue.push(
                await client.scroll({
                    scroll_id: response.body._scroll_id,
                    scroll: scrollTimeout
                })
            )
        }    
    }

    return sitemapFiles;
}

function writeSitemap(uriBuffer, type, number=0, index=false) {
    if(args.test) return '';
    let filename = 'sitemap_' + type + ((number > 0)? '_' + number : '') + '.xml'
    let content = '<?xml version="1.0" encoding="UTF-8"?>\n';
    if(index)
        content += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';
    else
        content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';

    uriBuffer.map(u => {
        if(index)
            content += '<sitemap><loc>' + u.uri + '</loc><lastmod>'+u.lastmod+'</lastmod></sitemap>\n';
        else {
            content += '<url><loc>' + u.uri + '</loc>\n';
        
            //Add lastmod
            content+='<lastmod>'+u.lastmod+'</lastmod>\n';
        
            //TODO: Adaptar a la frecuencia de corrida del ETL de contratos en cada caso
            content+='<changefreq>'+u.changefreq+'</changefreq>\n</url>\n';
        }
            
    });
    if(index)
        content += '</sitemapindex>';
    else
        content += '</urlset>';

    console.log('Writing:', filename);
    fs.writeFileSync('./sitemaps/' + filename, content);
    return filename;
}

function determineChangefreq(type, date) {
    let lastModDate = today;
    if(date)
        lastModDate = new Date(date);

    switch(type) {
        case 'proveedor':
            return 'monthly';
        case 'entidad':
        case 'contract':
            let daysDifference = Math.floor((today.getTime() - lastModDate.getTime()) / (1000 * 3600 * 24));
            if(daysDifference < 7) return 'daily';
            if(daysDifference < 30) return 'weekly';
            if(daysDifference < 90) return 'monthly';
            else return 'yearly';
    }
}

async function getProveedoresCache() {
    let cache = {}
    let docQuery = {
        "size": 0,
        "aggs": {
            "name": {
                "terms": {
                    "size": 1000000,
                    "field": "nit.keyword"
                },
                "aggs": {
                    "lastmod": {
                        "max": {
                            "field": "fecha_publicacion", 
                            "format": "yyyy-MM-dd'T'HH:mm:sszzz" // Investigar cómo se agrega la T
                        }
                    }
                }
            }
        }
    }
    let options = { 
        index: 'guatecompras_contratos',
        body: {} 
    }
    
    Object.assign(options.body, docQuery);
    const response = await client.search(options)
    let buckets = response.body.aggregations['name'].buckets;
    
    if(buckets.length > 0) {
        buckets.map( b => {
            cache[b.key] = b.lastmod.value_as_string;
        } );
    }

    return cache;
}