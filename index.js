const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const { encodeSitemapURL } = require('node-uri');
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
    { name: 'elasticUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'baseUrl', alias: 'b', type: String }
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
    
    console.log('Getting proveedores...')
    sitemaps.push(...await buildSitemaps('gt_proveedores', 'proveedor', query, 'nit', args.baseUrl + '/proveedor/'));
    
    console.log('Getting contracts...')
    sitemaps.push(...await buildSitemaps('gt_guatecompras', 'contract', query, 'nog_concurso', args.baseUrl + '/contract/'));

    console.log('Getting entidades...')
    query = {
        "size": 0, 
        "aggs": {
            "name": {
                  "terms": {
                      "size": 25000,
                      "field": "entidad_compradora.keyword" //equivale a dependencia, es el dato más importante
                  }
            }
        }
    }
    sitemaps.push(...await buildSitemaps('gt_guatecompras', 'entidad', query, 'name', args.baseUrl + '/entidad/', true)); 

    console.log(sitemaps);
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

async function buildSitemaps(index, type, docQuery, idField, location, aggs=false) {
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
            "fields": [ idField ]
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
                uriBuffer.push(encodeSitemapURL(location + id));

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
                    uriBuffer.push(encodeSitemapURL(location + id));
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

function writeSitemap(uris, type, number) {
    let filename = 'sitemap_' + type + '_' + number + '.xml'
    let content = '<?xml version="1.0" encoding="UTF-8"?>\n';
    content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';
    uris.map(u => {
        content += '<url><loc>' + u + '</loc></url>\n';
    });
    content += '</urlset>';

    console.log('Writing:', filename);
    fs.writeFileSync('./sitemaps/' + filename, content);
    return filename;
}