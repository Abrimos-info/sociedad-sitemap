const fs = require('fs');
const { Client } = require("@opensearch-project/opensearch");
const { encodeSitemapURL } = require('node-uri');
const commandLineArgs = require('command-line-args');
const optionDefinitions = [
    { name: 'dbUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'baseUrl', alias: 'b', type: String },
    { name: 'location', alias: 'l', type: String },
    { name: 'countries', alias: 'c', type: String },
    { name: 'country', alias: 'd', type: String },
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
const buyersIndex = 'sociedad_buyers';
const suppliersIndex = 'sociedad_suppliers';
const sitemapItemCount = 25000;
let countries = {}
let countryList = {};
let baseUrl = args.baseUrl + ( (!args.baseUrl.match(/\/$/))? '/' : '' );

run();

async function run() {
    console.log('Starting')
    
    console.log('Getting countries file...')
    countries = await getCountriesFile(args.countries);

    console.log('Getting buyers...')
    sitemaps.push(...await buildSitemaps(buyersIndex, 'buyer', query, 'id', 'updated_date', baseUrl));

    console.log('Getting suppliers...')
    sitemaps.push(...await buildSitemaps(suppliersIndex, 'supplier', query, 'id', 'updated_date', baseUrl));

    console.log('Generating countries sitemap...');
    sitemaps.push(buildCountrySitemap(baseUrl));
    
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

function buildSitemapIndex(filenames, base, location) {
    let uris = [];
    uris.push({uri: 'https://sociedad.info/sitemap-static.xml', lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")} );
    filenames.map( file => {
        uris.push({uri:  base + '/static/' + location + '/' + file, lastmod: new Date().toISOString("yyyy-MM-ddTHH:mm:sszzz")} );
        if(args.test) console.log('Index URI:', base + '/static/' + location + '/' + file);
    } );

    writeSitemap(uris, '', 'index', 0, true);
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
                "fields": [ idField, lastModField, 'country' ]
            }
    };
    Object.assign(options.body, docQuery);
    const response = await client.search(options)

    responseQueue.push(response)
    let lastmod, changefreq, country = null;

    while (responseQueue.length) {
        const response = responseQueue.shift()
        // collect the docs from this response
        for(let i=0; i<response.body.hits.hits.length; i++) {
            let hit = response.body.hits.hits[i];
            let id = '';
            let uri = '';
            if(hit.hasOwnProperty('fields') && hit.fields.hasOwnProperty(idField)) {
                id = hit.fields[idField][0];
                if (hit.fields[lastModField]) 
                    lastmod = hit.fields[lastModField][0];
                else 
                    lastmod = null;
                changefreq = determineChangefreq(type, lastmod);

                if( countries[hit.fields['country'][0]] ) {
                    countryCode = hit.fields['country'][0];
                    country = countries[countryCode].slug;
                    if(!countryList.hasOwnProperty(countryCode)) countryList[countryCode] = country;
                    
                    uri = location + country + '/' + type + '/' + id;
                    if(uri.length < 2048) {
                        if(!uriBuffer.hasOwnProperty(countryCode)) uriBuffer[countryCode] = { count: 1, uris: [] };
                        uriBuffer[countryCode].uris.push({uri: encodeSitemapURL(uri), lastmod: lastmod, changefreq: changefreq});
                        // if(args.test) console.log('Country:', country, 'URI:', uri, lastmod, changefreq);

                        if(uriBuffer[countryCode].uris.length == sitemapItemCount) {
                            if(args.test) console.log('Writing', type, 'sitemap for country:', countryCode, '#:', uriBuffer[countryCode].count);
                            sitemapFiles.push(writeSitemap(uriBuffer[countryCode].uris, countryCode, type, uriBuffer[countryCode].count));
                            uriBuffer[countryCode].count++;
                            uriBuffer[countryCode].uris = [];
                        }
                    }
                    
                }
            }
            allDocs++;
        }

        // check to see if we have collected all docs
        if(response.body.hits.total.value <= allDocs) {
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
        fs.writeFileSync('./sitemaps/' + filename, content);
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
    
    if(isNaN(dateObj) || dateObj.getFullYear() < 2000) { 
        console.log(date, dateObj);
        return false;
    }
    
    return true;
}