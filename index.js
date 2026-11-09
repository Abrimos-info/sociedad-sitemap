const fs = require('fs');
const path = require('path');
const { Client } = require("@opensearch-project/opensearch");
const { encodeSitemapURL } = require('node-uri');
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
    { name: 'dbUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'baseUrl', alias: 'b', type: String },
    { name: 'location', alias: 'l', type: String },
    { name: 'countries', alias: 'c', type: String },
    { name: 'country', alias: 'd', type: String },
    { name: 'test', alias: 't', type: Boolean, defaultValue: false },
    // Solo para pruebas: corta cada índice después de N documentos.
    { name: 'limit', alias: 'n', type: Number }
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
if(args.country) {
    query = {
        "query": {
            "match": {
                "country.keyword": args.country
            }
        }
    }
}
let sitemaps = [];
// Desde 2026-06 hay un alias por país (sociedad_suppliers_guatemala, _brasil, ...)
// y ya no existen sociedad_buyers ni sociedad_suppliers. Mismo patrón que usa el front.
const buyersIndex = 'sociedad_buyers_*';
const suppliersIndex = 'sociedad_suppliers_*';
const sitemapItemCount = 10000;
let countries = {}
let countryList = {};
let baseUrl = args.baseUrl + ( (!args.baseUrl.match(/\/$/))? '/' : '' );

// Los archivos se escriben en una carpeta temporal y reemplazan a la publicada
// recién al final, así una corrida que falla no deja una mezcla de archivos
// nuevos y viejos. static/sitemaps del front es un symlink a OUT_DIR.
const OUT_DIR = './sitemaps';
const TMP_DIR = './sitemaps.tmp';
const OLD_DIR = './sitemaps.old';
// Fecha de la última corrida en la que cambió algún sitemap de compradores o
// proveedores. Fuera de OUT_DIR para no publicarla. La lee el monitor del
// servidor: si pasan 3 días sin cambios, el ETL de origen probablemente se trabó.
const LAST_CHANGE_FILE = './sitemaps-last-change.txt';
let changedSitemaps = 0;

// Personas físicas de GT que no se listan. Misma regla que
// sociedad-web-front/lib/person-indexing.js, que les pone noindex: persona por
// classification, sin has_awards y sin aparecer en representatives.id de
// ninguna ficha. Si cambia allá, cambiarla acá.
// En ES e IT: toda persona física detectable por su identificador fiscal, con o
// sin contratos (opentender no trae classification ni has_awards).
const PERSON_CLASSIFICATIONS = new Set(['INDIVIDUAL', 'Persona Individual', 'Comerciante Individual']);
const ES_PERSON_ID = /^(0?[0-9]{8}-?[A-Z]|[XYZ][0-9]{7}-?[A-Z])$/i;
const IT_PERSON_ID = /^(IT)?[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$/i;
const ES_COMPANY_NAME = /(\bS\.?\s?L\.?(\s?[UP]\.?)?$|\bS\.?\s?A\.?(\s?U\.?)?$|\bU\.?T\.?E\b|\bSOCIEDAD\b|\bASOCIACI[OÓ]N\b|\bFUNDACI[OÓ]N\b|\bCOOPERATIVA\b|\bC\.?\s?B\.?$|\bLTDA?\b|\bLIMITED\b|\bS\.?L\.?\s|\bS\.?A\.?\s)/i;
let representativeIds = new Set();
let skippedPersons = 0;

run().catch(e => {
    console.error('ERROR: sitemap generation failed, published sitemaps left untouched.', e && (e.meta && JSON.stringify(e.meta.body) || e.stack || e));
    process.exit(1);
});

async function run() {
    console.log('Starting')

    console.log('Getting countries file...')
    countries = await getCountriesFile(args.countries);

    if(!args.test) {
        fs.rmSync(TMP_DIR, { recursive: true, force: true });
        fs.mkdirSync(TMP_DIR);
    }

    console.log('Getting buyers...')
    const buyerFiles = await buildSitemaps(buyersIndex, 'buyer', query, 'id', 'updated_date', baseUrl);
    sitemaps.push(...buyerFiles);

    console.log('Getting representatives...')
    representativeIds = await getRepresentativeIds(suppliersIndex);
    console.log('Representatives:', representativeIds.size);

    console.log('Getting suppliers...')
    const supplierFiles = await buildSitemaps(suppliersIndex, 'supplier', query, 'id', 'updated_date', baseUrl);
    sitemaps.push(...supplierFiles);
    console.log('Persons left out of the supplier sitemaps:', skippedPersons);

    if(buyerFiles.length == 0 || supplierFiles.length == 0)
        throw new Error('No buyer or supplier sitemaps were generated (buyers: ' + buyerFiles.length + ', suppliers: ' + supplierFiles.length + ')');

    console.log('Generating countries sitemap...');
    sitemaps.push(buildCountrySitemap(baseUrl));

    console.log('Generating sitemap index...');
    buildSitemapIndex(sitemaps, args.baseUrl, args.location);

    if(!args.test) {
        publish();
        if(changedSitemaps > 0) fs.writeFileSync(LAST_CHANGE_FILE, new Date().toISOString() + '\n');
        console.log('Changed buyer/supplier sitemaps:', changedSitemaps);
    }
    console.log('Finished');
}

// Reemplaza la carpeta publicada por la nueva con dos renames. Los archivos que
// ya no se generan (por ejemplo si bajó la cantidad de sitemaps) desaparecen.
function publish() {
    fs.rmSync(OLD_DIR, { recursive: true, force: true });
    if(fs.existsSync(OUT_DIR)) fs.renameSync(OUT_DIR, OLD_DIR);
    fs.renameSync(TMP_DIR, OUT_DIR);
    fs.rmSync(OLD_DIR, { recursive: true, force: true });
    console.log('Published', sitemaps.length + 1, 'files to', OUT_DIR);
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

// Todos los ids que aparecen como representante en alguna ficha, de cualquier país,
// igual que la relación representatives.id -> id de la ficha de proveedor del front.
async function getRepresentativeIds(index) {
    const ids = new Set();
    let after = undefined;
    while(true) {
        const response = await client.search({
            index: index,
            body: {
                size: 0,
                aggs: {
                    reps: {
                        composite: {
                            size: 10000,
                            sources: [ { id: { terms: { field: 'representatives.id.keyword' } } } ],
                            ...(after && { after: after })
                        }
                    }
                }
            }
        });
        const agg = response.body.aggregations.reps;
        agg.buckets.forEach(b => ids.add(b.key.id));
        if(!agg.after_key || agg.buckets.length == 0) break;
        after = agg.after_key;
    }
    return ids;
}

function isDeindexedPerson(fields, id) {
    const country = fields['country'] && fields['country'][0];
    const classification = fields['classification'] && fields['classification'][0];
    const hasAwards = fields['has_awards'] && fields['has_awards'][0];
    const identifier = ((fields['identifier'] && fields['identifier'][0]) || '').trim();
    const name = (fields['name'] && fields['name'][0]) || '';
    if(country == 'ES') return ES_PERSON_ID.test(identifier) && !ES_COMPANY_NAME.test(name);
    if(country == 'IT') return IT_PERSON_ID.test(identifier);
    if(country != 'GT') return false;
    if(!PERSON_CLASSIFICATIONS.has(classification)) return false;
    if(hasAwards === true) return false;
    return !representativeIds.has(id);
}

function buildCountrySitemap(base) {
    let sitemapFiles = [];
    let list = [];
    Object.keys(countryList).map( code => {
        let country = countryList[code];
        list.push({uri:  base + country, lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")})
    } )

    sitemapFiles.push(writeSitemap(list, '', 'countries', 0));
    return sitemapFiles;
}

// El lastmod de cada sitemap hijo cambia solo si cambió su contenido. Si se
// pusiera la fecha de la corrida en todos, Google aprende a ignorarlo.
function buildSitemapIndex(filenames, base, location) {
    const now = new Date().toISOString();
    const previous = readPreviousIndexLastmods();
    let uris = [];
    let unchanged = 0;
    uris.push({uri: 'https://sociedad.info/sitemap-static.xml', lastmod: now} );
    filenames.map( file => {
        const uri = base + '/static/' + location + '/' + file;
        let lastmod = now;
        if(!args.test && previous[uri] && sameContent(file)) {
            lastmod = previous[uri];
            unchanged++;
        }
        else if(file != 'sitemap_countries.xml') {
            // El de países cambia siempre (su lastmod es la fecha de la corrida).
            changedSitemaps++;
        }
        uris.push({uri: uri, lastmod: lastmod} );
        if(args.test) console.log('Index URI:', uri);
    } );
    console.log('Sitemaps unchanged since last run:', unchanged, 'of', filenames.length);

    writeSitemap(uris, '', 'index', 0, true);
}

function readPreviousIndexLastmods() {
    const lastmods = {};
    try {
        const xml = fs.readFileSync(path.join(OUT_DIR, 'sitemap_index.xml'), 'utf8');
        const re = /<loc>([^<]+)<\/loc><lastmod>([^<]+)<\/lastmod>/g;
        let m;
        while((m = re.exec(xml)) !== null) lastmods[m[1]] = m[2];
    }
    catch(e) {
        console.log('No previous sitemap index, every lastmod is new.');
    }
    return lastmods;
}

function sameContent(file) {
    try {
        return fs.readFileSync(path.join(OUT_DIR, file)).equals(fs.readFileSync(path.join(TMP_DIR, file)));
    }
    catch(e) {
        return false;
    }
}

async function buildSitemaps(index, type, docQuery, idField, lastModField, location) {
    let allDocs = 0;
    let sitemapFiles = [];
    let uriBuffer = {};
    const responseQueue = []

    let options = {
            "index": index,
            "scroll": scrollTimeout,
            "size": batchSize,
            "_source": false,
            "body": {
                "fields": [ idField, lastModField, 'country', 'classification', 'has_awards', 'member_of.id', 'identifier', 'name' ]
            }
    };
    Object.assign(options.body, docQuery);
    const response = await client.search(options)

    responseQueue.push(response)
    let lastmod, changefreq, country = null;
    let scrollId = null;

    while (responseQueue.length) {
        const response = responseQueue.shift()
        scrollId = response.body._scroll_id;
        // collect the docs from this response
        for(let i=0; i<response.body.hits.hits.length; i++) {
            let hit = response.body.hits.hits[i];
            let id = '';
            let uri = '';
            allDocs++;
            if(hit.hasOwnProperty('fields') && hit.fields.hasOwnProperty(idField)) {
                id = hit.fields[idField][0];
                if (hit.fields[lastModField])
                    lastmod = hit.fields[lastModField][0];
                else
                    lastmod = null;
                changefreq = determineChangefreq(type, lastmod);

                if(type == 'supplier' && isDeindexedPerson(hit.fields, id)) {
                    skippedPersons++;
                    continue;
                }

                if( hit.fields['country'] && countries[hit.fields['country'][0]] ) {
                    const countryCode = hit.fields['country'][0];
                    country = countries[countryCode].slug;
                    if(!countryList.hasOwnProperty(countryCode)) countryList[countryCode] = country;

                    // Unidad compradora: su URL propia es la anidada bajo la entidad.
                    // /buyer/{uc} responde 301 a esta (sociedad-web-front, itemPage).
                    const parentId = hit.fields['member_of.id'] && hit.fields['member_of.id'][0];
                    const isBuyerUnit = type == 'buyer' && hit.fields['classification'] && hit.fields['classification'][0] == 'buyer_unit' && parentId;
                    if(isBuyerUnit)
                        uri = location + country + '/buyer/' + parentId + '/buyer-unit/' + id;
                    else
                        uri = location + country + '/' + type + '/' + id;
                    if(uri.length < 2048) {
                        if(!uriBuffer.hasOwnProperty(countryCode)) uriBuffer[countryCode] = { count: 1, uris: [] };
                        uriBuffer[countryCode].uris.push({uri: encodeSitemapURL(uri), lastmod: lastmod, changefreq: changefreq});

                        if(uriBuffer[countryCode].uris.length == sitemapItemCount) {
                            sitemapFiles.push(writeSitemap(uriBuffer[countryCode].uris, countryCode, type, uriBuffer[countryCode].count));
                            uriBuffer[countryCode].count++;
                            uriBuffer[countryCode].uris = [];
                        }
                    }

                }
            }
        }

        // check to see if we have collected all docs
        const reachedLimit = args.limit && allDocs >= args.limit;
        if(response.body.hits.total.value <= allDocs || response.body.hits.hits.length == 0 || reachedLimit) {
            // Write all remaining country sitemaps
            Object.keys(uriBuffer).map( countryCode => {
                let list = uriBuffer[countryCode].uris;
                if(list.length > 0) {
                    if(args.test) console.log('Writing', type, 'sitemap for country:', countryCode, '#:', uriBuffer[countryCode].count);
                    sitemapFiles.push(writeSitemap(list, countryCode, type, uriBuffer[countryCode].count));
                }
            } );

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

    if(scrollId) {
        try { await client.clearScroll({ body: { scroll_id: scrollId } }); }
        catch(e) { console.error('clearScroll', e.message); }
    }
    console.log(type, 'documents read:', allDocs, 'sitemaps:', sitemapFiles.length);

    return sitemapFiles;
}

function writeSitemap(uriList, country, type, number=0, index=false) {
    let filename = 'sitemap_' + (country? country + '_' : '') + type + ((number > 0)? '_' + number : '') + '.xml'
    let content = '<?xml version="1.0" encoding="UTF-8"?>\n';
    if(index)
        content += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';
    else
        content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n';

    uriList.map(u => {
        if(index)
            content += '<sitemap><loc>' + u.uri + '</loc><lastmod>'+u.lastmod+'</lastmod></sitemap>\n';
        else {
            content += '<url><loc>' + u.uri + '</loc>\n';

            if(u.lastmod && isValidSitemapDate(u.lastmod))
                content+='<lastmod>'+u.lastmod+'</lastmod>\n';

            if(!index) {
                if(u.changefreq)
                    content+='<changefreq>'+u.changefreq+'</changefreq>\n';
                content+='</url>\n';
            }
        }

    });
    if(index)
        content += '</sitemapindex>';
    else
        content += '</urlset>';

    console.log('Writing:', filename);
    if(!args.test)
        fs.writeFileSync(path.join(TMP_DIR, filename), content);
    return filename;
}

function determineChangefreq(type, date) {
    let lastModDate = today;
    if(date)
        lastModDate = new Date(date);

    switch(type) {
        case 'supplier':
            return 'monthly';
        case 'buyer':
            let daysDifference = Math.floor((today.getTime() - lastModDate.getTime()) / (1000 * 3600 * 24));
            if(daysDifference < 7) return 'daily';
            if(daysDifference < 30) return 'weekly';
            if(daysDifference < 90) return 'monthly';
            else return 'yearly';
    }
}

async function getCountriesFile(path) {
    let data = null;
    try{
        data = JSON.parse(fs.readFileSync(path, 'utf8'));
    }
    catch(e) {
        console.error('Error trying to read countries file:', e);
        process.exit(2);
    }

    return data;
}

function isValidSitemapDate(date) {
    let dateObj = new Date(date);

    if(isNaN(dateObj) || dateObj.getFullYear() < 2000) return false;

    return true;
}
