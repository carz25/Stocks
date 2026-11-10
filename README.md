# StockPulse

A live stock watchlist that runs entirely in the browser. There is no server: each visitor pastes their own [Finnhub](https://finnhub.io) API key, which is stored only in that browser and sent only to finnhub.io.

## Pages

- **Home**: a pulse line with one spike per stock, a ticker tape, breadth, movers and the best entry scores.
- **Watchlist**: a sortable list or a card grid, with price freshness, today's path, 52-week range, analyst ratings and a "Good time to invest?" score you can expand.
- **Heatmap**: tiles sized by market cap and coloured by the move over 1 day, 5 days or 13 weeks.
- **Stock detail**: today's chart, key stats, performance, news, the score breakdown and analyst consensus.
- **Configuration**: API key, watchlist symbols and a feed log. The dot on the button is green when live, yellow on a Finnhub rate limit, red on an error or a missing key.

## Run it

Open `index.html` in a browser, or serve the folder with any static server. Click **Configuration**, paste a Finnhub key and press **Save key**.

## Publish on GitHub Pages

1. Push these files to a GitHub repository.
2. In the repository, open **Settings > Pages**.
3. Under **Build and deployment**, choose **Deploy from a branch**, pick the branch and the folder that holds `index.html`, then save.
4. The site appears at `https://<user>.github.io/<repo>/` after a minute or two.

## Limits to know about

- **Free Finnhub plan**: 60 requests a minute. The app spaces its calls and pauses for 30 seconds on a rate-limit response. Loading fundamentals for a full list takes about a minute the first time, then they are cached for 12 hours.
- **Canadian (.TO) symbols** need a Finnhub plan that includes them. Otherwise they show "Not included in your Finnhub plan".
- **No price history** on the free plan, so the chart on the detail page is built from updates received while the page is open.
- **The score is rule-based**, built from analyst ratings, trend and valuation. It is not a forecast and not investment advice.
- The key is kept in the browser's local storage. Anyone with access to that browser profile can read it, so do not use a shared computer.
