const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

async function writeToCsv(data, outputFile) {
    let success = false;
    while (!success) {

        if (!data || data.length === 0) {
            throw new Error("No data to write!");
        }
        const fileExists = fs.existsSync(outputFile);
    
        if (!(data instanceof Array)) {
            data = [data]
        }
    
        const headers = Object.keys(data[0]).map(key => ({id: key, title: key}))
    
        const csvWriter = createCsvWriter({
            path: outputFile,
            header: headers,
            append: fileExists
        });
        try {
            await csvWriter.writeRecords(data);
            success = true;
        } catch (e) {
            console.log("Failed data", data);
            throw new Error("Failed to write to csv");
        }
    }
}


async function crawlProfiles(browser, keyword, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const firstName = keyword.split(" ")[0];
        const lastName = keyword.split(" ")[1]
        

        const page = await browser.newPage();
        try {
            const url = `https://www.linkedin.com/pub/dir?firstName=${firstName}&lastName=${lastName}&trk=people-guest_people-search-bar_search-submit`;
    
            await page.goto(url);

            console.log(`Successfully fetched: ${url}`);

            const divCards = await page.$$("div[class='base-search-card__info']");

            for (const divCard of divCards) {

                const link = await page.evaluate(element => element.parentElement.getAttribute("href"), divCard);
                const splitLink = link.split("/")
                const name = splitLink[splitLink.length-1].split("?")[0];
                
                const displayNameElement = await divCard.$("h3[class='base-search-card__title']");
                const displayName = await page.evaluate(element => element.textContent, displayNameElement);

                const locationElement = await page.$("p[class='people-search-card__location']");
                const location = await page.evaluate(element => element.textContent, locationElement);

                let companies = "n/a";

                const hasCompanies = await page.$("span[class='entity-list-meta__entities-list']");

                if (hasCompanies) {
                    companies = await page.evaluate(element => element.textContent, hasCompanies);
                }


                const searchData = {
                    name: name.trim(),
                    display_name: displayName.trim(),
                    url: link.trim(),
                    location: location.trim(),
                    companies: companies.trim()
                };

                await writeToCsv([searchData], `${keyword.replace(" ", "-")}.csv`);
            }

            success = true;

        } catch (err) {
            console.log(`Error: ${err}, tries left ${retries - tries}`);
            tries++;

        } finally {
            await page.close();
        } 
    }
}

async function startCrawl(keywordList, location, concurrencyLimit, retries) {

    const browser = await puppeteer.launch();

    while (keywordList.length > 0) {
        const currentBatch = keywordList.splice(0, concurrencyLimit);
        const tasks = currentBatch.map(keyword => crawlProfiles(browser, keyword, location, retries));

        try {
            await Promise.all(tasks);
        } catch (err) {
            console.log(`Failed to process batch: ${err}`);
        }
    }

    await browser.close();
}


async function main() {
    const keywords = ["bill gates", "elon musk"];
    const concurrencyLimit = 5;
    const location = "us";
    const retries = 3;
    const aggregateFiles = [];

    console.log("Crawl starting");
    console.time("startCrawl");
    for (const keyword of keywords) {
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }
    await startCrawl(keywords, location, concurrencyLimit, retries);        
    console.timeEnd("startCrawl");
    console.log("Crawl complete");
}


main();