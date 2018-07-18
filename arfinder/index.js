const puppeteer = require('puppeteer');
const readline = require('readline');
const Kafka = require('node-rdkafka');
const producer = new Kafka.Producer({
    'metadata.broker.list': '127.0.0.1:9092'
});
producer.connect();

const searchUrl = 'http://www.arbookfind.com/default.aspx';

var lines = require('fs').readFileSync('./book_isbn.csv', 'utf-8').split('\n');

const cookies = [
    {
        'name': 'ASP.NET_SessionId',
        'value': 'glfe5145zejwupaeuwxrlo55',
        'domain': '.arbookfind.com',
        'path': '/'
    },
    {
        'name': 'BFUserType',
        'value': 'Parent',
        'domain': '.arbookfind.com',
        'path': '/'
    },
];

let crawl = async (page, isbn) => {
    try {
        if (!page.url().startsWith(searchUrl)) {
            await page.goto(searchUrl, {
                timeout: 0,
                waitUntil: "networkidle2"
            });
        }
        await page.type('#ctl00_ContentPlaceHolder1_txtKeyWords', '', {
            delay: 10
        });
        await page.type('#ctl00_ContentPlaceHolder1_txtKeyWords', isbn, {
            delay: 10
        });

        await Promise.all([
            page.waitForNavigation({
                timeout: 0,
                waitUntil: "networkidle2"
            }),
            page.click('#ctl00_ContentPlaceHolder1_btnDoIt', {
                delay: 10
            })
        ]);

        const href = await page.evaluate(() => {
            const target = document.querySelector('#ctl00_ContentPlaceHolder1_ucSeachResults_pnlQuizListing #book-title');
            let href = null;
            if (target) {
                href = target.getAttribute('href');
            }
            return href;
        });
        if (href) {
            await Promise.all([
                page.waitForNavigation({
                    timeout: 0,
                    waitUntil: "networkidle2"
                }),
                await page.goto('http://www.arbookfind.com/' + href, {
                    timeout: 0,
                    waitUntil: "networkidle2"
                })
            ]);
            const data = await page.evaluate(() => {
                const arScore = document.querySelector('#ctl00_ContentPlaceHolder1_ucBookDetail_lblPoints').innerText;
                const level = document.querySelector('#ctl00_ContentPlaceHolder1_ucBookDetail_lblBookLevel').innerText;
                const wordCount = document.querySelector('#ctl00_ContentPlaceHolder1_ucBookDetail_lblWordCount').innerText;
                const topicStr = document.querySelector('#ctl00_ContentPlaceHolder1_ucBookDetail_lblTopicLabel').innerText;
                const serieStr = document.querySelector('#ctl00_ContentPlaceHolder1_ucBookDetail_lblSeriesLabel').innerText;

                const seriesList = [];
                const genresList = [];
                if (topicStr) {
                    for (let name of topicStr.split(';')) {
                        name = name.trim();
                        if (name) {
                            genresList.push({
                                name
                            });
                        }
                    }
                }
                if (serieStr) {
                    for (let name of serieStr.split(';')) {
                        name = name.trim();
                        if (name) {
                            seriesList.push({
                                name
                            });
                        }
                    }
                }
                return {
                    seriesList: seriesList,
                    genresList: genresList,
                    arScore,
                    level,
                    wordCount,
                };
            });
            data.isbn13 = isbn;
            await page.goBack({
                timeout: 0,
                waitUntil: 'networkidle2',
            });
            return data;
        } else {
            return false;
        }
    } catch (err) {
        console.log(err);
    }
};

const processData = async (page, idx, isbn) => {
    const data = await crawl(page, isbn);
    if (data) {
        producer.produce(
            'scrapy_kafka_item',
            // optionally we can manually specify a partition for the message
            // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
            null,
            // Message to send. Must be a buffer
            new Buffer(JSON.stringify({
                cname: "com.gego.edu.book.bean.BookModifyBean",
                data: data
            })),
        );
    }
    return data;
};

producer.on('ready', async () => {
    const browser = await puppeteer.launch({
        headless: false,
        executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    });
    const pages = [];
    const step = 5;
    for (let i = 0; i < step; ++i) {
        await (async () => {
            const page = await browser.newPage();
            page.setViewport({
                width: 1280,
                height: 720
            });
            await page.setCookie(...cookies);
            await page.goto(searchUrl, {
                timeout: 0,
                waitUntil: "networkidle2"
            });
            pages.push(page);
        })();
    }
    try {
        let successTotal = 0;
        lines = lines.slice(0, 12);
        const startTs = new Date().getTime();
        for (let idx = 0; idx < lines.length; idx += step) {
            console.log(idx + 1, ' ===> ', idx + step);
            console.log('speed: ', ((idx + 1) / (Math.ceil((new Date().getTime() - startTs) / 1000))).toFixed(2));
            const requests = [];
            for (let i = 0; i < Math.min(lines.length - idx, step); ++i) {
                requests.push(processData(pages[i], idx + i, lines[idx + i]));
            }
            const result = await Promise.all(requests);
            // const result = await Promise.all([
            //     processData(pages[0], idx, lines[idx]),
            //     processData(pages[1], idx + 1, lines[idx + 1]),
            //     processData(pages[2], idx + 2, lines[idx + 2]),
            //     processData(pages[3], idx + 3, lines[idx + 3]),
            //     processData(pages[4], idx + 4, lines[idx + 4]),
            // ]);
            // console.log(result);
            result.forEach((item) => {
                if (item) {
                    successTotal += 1;
                }
            });
            console.log('succeed: ', successTotal);
        }
        producer.disconnect();
        await browser.close();
    } catch (err) {
        console.log(err);
    }
});
