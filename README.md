# Portfolio Analyzer

Portfolio Analyzer is a Streamlit-based application designed to help users analyze their stock portfolios by uploading tradebooks. The tool processes historical market data, adjusts for corporate actions and visualizes portfolio performance over time. It provides insights into portfolio holdings, valuation trends, and the latest holding patterns through interactive charts to help users make informed decisions.

---

## Features

- **Tradebook Upload**: Upload tradebooks in CSV format for supported brokers.
- **Corporate Action Adjustments**: Automatically adjusts holdings for stock splits and bonuses.
- **Historical Data Integration**: Fetches historical stock prices for portfolio symbols.
- **Interactive Visualizations**:
  - Portfolio holdings over time.
  - Portfolio valuation trends.
  - Latest holdings pattern (pie chart).
- **Streamlit UI**: Intuitive and user-friendly interface for seamless interaction.

---

## Installation

Follow these steps to set up and run the Portfolio Analyzer on your prefered infrastructure:

### Prerequisites

Ensure the following tools are installed on your system:
- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Python 3.12+**: [Install Python](https://www.python.org/downloads/)
- **pip**: Comes pre-installed with Python.
- **Git**: [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

### Clone the Repository

```bash
git clone https://github.com/your-username/portfolio-analyzer.git
cd portfolio-analyzer
```

### Set envrionment variables

Create a `.env` file and set values to following variables:

```bash
AIRFLOW_ADMIN_EMAIL=""
AIRFLOW_ADMIN_FIRST_NAME=""
AIRFLOW_ADMIN_LAST_NAME=""
AIRFLOW_ADMIN_PASSWORD=""
AIRFLOW_ADMIN_USERNAME=""
AIRFLOW_WEBSERVER_SECRET_KEY=""
POSTGRES_AIRFLOW_DATABASE=""
POSTGRES_AIRFLOW_PASSWORD=""
POSTGRES_AIRFLOW_USERNAME=""
POSTGRES_USERNAME=""
POSTGRES_PASSWORD=""
POSTGRES_PORTFOLIOANALYZER_DATABASE=""
POSTGRES_PORTFOLIOANALYZER_PASSWORD=""
POSTGRES_PORTFOLIOANALYZER_USERNAME=""
```

### Build the Application

Run the provided `build.sh` script to set up the application:

```bash
[ -f .env ] && set -a && source .env && set +a; bash build.sh
```

This will:

1. Load the environment variables declared in `.env` 

2. Build Docker images for the application and its dependencies.

3. Create necessary configuration files for Airflow, PostgreSQL, and Streamlit.

#### ⚠️ Warning:

**If no `.env` file is found, the `build.sh` script will continue and use default values for all environment variables. This may result in insecure credentials.**

### Running the Application

Start the Application
Use Docker Compose to start all services:

```bash
docker compose up -d
```

This will start the following services:

| Service           | URL                      | Description                         |
|-------------------|--------------------------|-------------------------------------|
| Streamlit         | http://localhost:8501    | UI for uploading and analyzing data |
| Airflow Webserver | http://localhost:8080    | DAG orchestration and scheduling    |
| PostgreSQL        | http://localhost:5432    | Stores data from stock exchange     |
| Redis             | http://localhost:6379    | Message broker for Airflow          |

Access the Airflow Webser

- Open your browser and navigate to http://localhost:8080.
- Unpause the `get_securities_from_bse` DAG
- Wait for the data ingetion process to complete.
- This will create the `securities` table in the database required by the application.

Access the Streamlit Application

 - Open your browser and navigate to http://localhost:8501.
 - On the Home page: 
    - Select your broker (e.g., Zerodha).
    - Select the segment (e.g., Fully Paid Equity Shares).
    - Upload your tradebook in CSV format.
    - Once the tradebook is uploaded, the application will process the data and redirect you to the Holdings page.

View Portfolio Insights on the Holdings page

- Portfolio Holdings Over Time: Interactive line chart showing stock holdings over time.

 - Portfolio Valuation Over Time: Line chart showing the total portfolio valuation trend.

- Latest Holdings Pattern: Pie chart showing the distribution of holdings as of the latest date.

### Tech Stack

- Frontend: Streamlit

- Backend: Apache Airflow, Apache Spark, pandas, Python

- Database: PostgreSQL

- Messaging: Redis

- Containerization: Docker, Docker Compose

---

## 🚧 Development Roadmap

**This application is under active development. You may encounter bugs, missing features, or inconsistent behavior as functionality is being built out and refined. Expect frequent updates, improvements, and new capabilities.**

### Upcoming Features

1. Asset Class Expansion

- Support for Mutual Funds
- Integration of Sovereign Bonds and Government Securities

2. Import Support

- Broaden compatibility across additional brokers
- Parse Consolidated Account Statements (CAS) from NSDL and CDSL depositories

3. Advanced Analytics

- Absolute returns (overall, by stock, by segment)
- XIRR (Extended Internal Rate of Return) at portfolio, segment, and security levels
- Cumulative return vs benchmark indices (e.g., Nifty 50, S&P 500)
- Alpha, Beta, and risk-adjusted metrics (Sharpe, Sortino ratios)
- Drawdown analysis with recovery timelines
- Volatility heatmaps (by weekday and month)
- Rolling returns (1M, 3M, 6M, 1Y windows)
- Rebalancing recommendations based on asset allocation drift
