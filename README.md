# World Bank Development Analytics

An end-to-end data pipeline project analyzing global socio-economic development indicators (GDP, health, education, environment) using the World Bank Open Data API, built with Apache Spark (Databricks) following the Medallion Architecture (Bronze → Silver → Gold).

## Project Overview

This project ingests country-level development data from the World Bank API, processes it through Bronze, Silver, and Gold layers, and culminates in a business intelligence dashboard analyzing global development trends.

## Data Source

- **API**: [World Bank Open Data API](https://api.worldbank.org/v2/)
- **Base endpoint**: `https://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json`
- **No authentication required**

### Indicators Used
| Indicator | Code |
|---|---|
| GDP (current US$) | `NY.GDP.MKTP.CD` |
| GDP per capita | `NY.GDP.PCAP.CD` |
| Population, total | `SP.POP.TOTL` |
| Poverty headcount ($2.15/day) | `SI.POV.DDAY` |
| Life expectancy at birth | `SP.DYN.LE00.IN` |
| Literacy rate | `SE.ADT.LITR.ZS` |
| Health expenditure (% GDP) | `SH.XPD.CHEX.GD.ZS` |
| CO2 emissions per capita | `EN.ATM.CO2E.PC` |
| Inflation (%) | `FP.CPI.TOTL.ZG` |
| Unemployment (%) | `SL.UEM.TOTL.ZS` |

## Ingestion Pattern

- **Full Load**: Complete historical time series (1990–2024) for all selected indicators across all countries.
- **Incremental Load**: Re-pull of the latest 1–2 reporting years to detect newly available records and revised (corrected) values — since World Bank data is periodically updated rather than streamed daily.

## Repository Structure
