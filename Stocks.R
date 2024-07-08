#' Function for checking if a package is installed, and installing it if not
install <- function(packages) {
  to_install <- packages[which(!packages %in% rownames(installed.packages()))]
  install.packages(to_install)
}

install(packages = c("duckdb", "tidyverse", "stringr"))

library(duckdb)
library(tidyverse)
library(stringr)

# list of URLs containing our csv files
base_url <- "https://raw.githubusercontent.com/Robot-Wealth/r-quant-recipes/master/data/"
stocks <- c("AAPL", "BA", "CAT", "IBM", "MSFT")
urls <- str_glue("{base_url}{stocks}.csv")

# read data into a dataframe
prices_df <- urls %>%
  map_dfr(read_csv, col_types = "Dddddddc") %>%
  arrange(Date, Ticker)

head(prices_df)

# make an in-memory db and store the connection in a variable
# con <- dbConnect(duckdb::duckdb())

# to make a persistent db on disk (and to connect to it again later) do:
con <- dbConnect(duckdb::duckdb(dbdir = "mydatabase.duckdb"))

# write our prices data to duckdb table
table_name <- "prices"
duckdb::dbWriteTable(con, table_name, prices_df)

# remove prices_df
rm(prices_df)


tbl(con, "prices") %>%
  group_by(Ticker) %>%
  summarise(
    Count = n(),
    From = first(Date),
    To = last(Date)
  )


result <- dbGetQuery(con, paste0("SELECT * FROM prices LIMIT 5"))
result

tbl(con, "prices") %>%
  head(5) %>%
  collect()


# Basic TWAP approximation

# won't work
# tbl(con, "prices") %>%
#    dplyr::mutate(TWAP = mean(c(Open, High, Low, Close))) %>%
#    head()

# will work
tbl(con, "prices") %>%
  dplyr::mutate(TWAP = (Open + High + Low + Close)/4) %>%
  head()

# 60-day moving average
affected_rows <- dbExecute(con,
                           'ALTER TABLE prices ADD COLUMN IF NOT EXISTS MA60 DOUBLE;
     UPDATE prices SET MA60 = t2.MA60
     FROM (
        SELECT Ticker, Date, AVG("Adj Close") OVER (
            PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS MA60
    FROM prices
    ) t2
WHERE prices.Ticker = t2.Ticker AND prices.Date = t2.Date;'
)

affected_rows


tbl(con, "prices") %>% head()

# Set chart options
options(repr.plot.width = 14, repr.plot.height=7)
theme_set(theme_bw())
theme_update(text = element_text(size = 20))

# plot
tbl(con, "prices") %>%
  select(Date, Ticker, `Adj Close`, MA60) %>%
  pivot_longer(cols = c(`Adj Close`, MA60), names_to = "Type", values_to = "Price") %>%
  ggplot(aes(x = Date, y = Price, colour = Type)) +
  geom_line() +
  facet_wrap(~Ticker) +
  labs(title = "Closing prices and their 60-day moving average")


