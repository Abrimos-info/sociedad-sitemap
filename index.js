const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const { encodeSitemapURL } = require('node-uri');
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
    { name: 'elasticUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'baseUrl', alias: 'b', type: String },
    { name: 'location', alias: 'l', type: String }
];
const args = commandLineArgs(optionDefinitions);
if(!args.elasticUri || !args.baseUrl) {
    console.error("ERROR: missing params.");
    process.exit(1);
}

const elasticNode = args.elasticUri;
let client = getClient(elasticNode);
const batchSize = 10000;
const scrollTimeout = '600s';
let query = {
    "query": {
        "match_all": {}
    }
}
let sitemaps = [];

run();

async function run() {
    console.log('Starting')
    
    console.log('Generating static sitemap...');
    buildStaticSitemap(args.baseUrl);
    sitemaps.push('sitemap_static.xml');

    console.log('Getting proveedores...')
    sitemaps.push(...await buildSitemaps('gt_proveedores', 'proveedor', query, 'nit', 'fecha_sat', args.baseUrl + '/guatemala/proveedor/'));
    
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
                            "format": "yyyy-MM-dd HH:mm:sszzz"
                        }
                    }  
                }
            }
        }
    }
    sitemaps.push(...await buildSitemaps('gt_guatecompras', 'entidad', query_entidad, 'name', 'lastmod', args.baseUrl + '/guatemala/entidad/', true)); 

    console.log('Getting contracts...')
    sitemaps.push(...await buildSitemaps('gt_guatecompras', 'contract', query, 'nog_concurso', 'fecha_publicacion', args.baseUrl + '/guatemala/contract/'));

    console.log('Generating sitemap index...');
    buildSitemapIndex(sitemaps, args.baseUrl, args.location);
    console.log('Finished');
}


function getClient(elasticNode) {
    let client = null;
    try {
        client = new Client({ node: elasticNode, requestTimeout: 60000, maxRetries: 10, sniffOnStart: true, tls: { rejectUnauthorized: false }, resurrectStrategy: "none", compression: "gzip" })
    }
    catch (e) {
        console.error("getClient",e);
    }
    return client;
}

function buildSitemapIndex(filenames, base, location) {
    let uris = [];
    filenames.map( file => {
        uris.push({uri:  base + '/' + location + '/' + file, lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")} );
    } );

    writeSitemap(uris, 'index', 0, true);
}

function buildStaticSitemap(base) {
    let uris = [];
    
    uris.push({uri: base + '/', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/buscador', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/acerca-de', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});
    uris.push({uri: base + '/privacidad', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")});

    writeSitemap(uris, 'static');
}

async function buildSitemaps(index, type, docQuery, idField, lastModField, location, aggs=false) {
    let allDocs = 0;
    let sitemapFiles = [];
    let sitemapCount = 0;
    let uriBuffer = [];
    const responseQueue = []

    let options = {}
    if(!aggs) {
        options = {
            "index": index,
            "scroll": scrollTimeout,
            "size": batchSize,
            "_source": false,
            "fields": [ idField, lastModField ]
        };
    }
    Object.assign(options, docQuery);
    const response = await client.search(options)

    responseQueue.push(response)

    if(aggs) {
        const data = responseQueue.shift();
        let buckets = data.aggregations[idField].buckets;
        if(buckets.length > 0) {
            buckets.map(b => {
                allDocs++;
                let id = b.key;
                let lastmod = b.lastmod.value_as_string;
                uriBuffer.push({uri: encodeSitemapURL(location + id), lastmod: lastmod});

                if(allDocs % 50000 == 0) {
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
            const body = responseQueue.shift()
            // collect the docs from this response
            for(let i=0; i<body.hits.hits.length; i++) {
                let hit = body.hits.hits[i];
                let id = '';
                if(hit.hasOwnProperty('fields') && hit.fields.hasOwnProperty(idField)) {
                    id = hit.fields[idField][0];
                    lastmod = hit.fields[lastModField][0];
                    uriBuffer.push({uri: encodeSitemapURL(location + id), lastmod: lastmod});
                }
                allDocs++;
                if(allDocs % 50000 == 0) {
                    sitemapCount++;
                    sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
                    uriBuffer = [];
                }
            }
    
            // check to see if we have collected all docs
            if(body.hits.total.value <= allDocs) {
                sitemapCount++;
                sitemapFiles.push(writeSitemap(uriBuffer, type, sitemapCount));
                uriBuffer = [];
                break;
            }
    
            // get the next response if there are more docs to fetch
            responseQueue.push(
                await client.scroll({
                    scroll_id: body._scroll_id,
                    scroll: scrollTimeout
                })
            )
        }    
    }

    return sitemapFiles;
}

function writeSitemap(uriBuffer, type, number=0, index=false) {
    let filename = 'sitemap_' + type + ((number > 0)? '_' + number : '') + '.xml'
    let content = '<?xml version="1.0" encoding="UTF-8"?>\n';
    if(index)
        content += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';
    else
        content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';

    uriBuffer.map(u => {
        if(index)
            content += '<sitemap><loc>' + u.uri + '</loc><lastmod>'+u.lastmod+'</lastmod></sitemap>\n';
        else
            content += '<url><loc>' + u.uri + '</loc>\n';
        
            //Add lastmod
            content+='<lastmod>'+u.lastmod+'</lastmod>\n';
        
            //TODO: Adaptar a la frecuencia de corrida del ETL de contratos en cada caso
            content+='<changefreq>weekly</changefreq>\n</url>\n';
            
    });
    if(index)
        content += '</sitemapindex>';
    else
        content += '</urlset>';

    console.log('Writing:', filename);
    fs.writeFileSync('./sitemaps/' + filename, content);
    return filename;
}