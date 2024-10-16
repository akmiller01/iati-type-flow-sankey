#### Setup ####
list.of.packages <- c("rstudioapi", "data.table", "dplyr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd <- dirname(getActiveDocumentContext()$path) 
setwd(wd)
setwd("../")

org_type_codelist = fread("input/OrganisationType.csv")
org_type_map = org_type_codelist$name
names(org_type_map) = as.character(org_type_codelist$code)

ex_rates = fread("input/ex_rates.csv")
setnames(ex_rates,
         c("year", "cc", "ex.rate"),
         c("transaction_year", "currency", "multiplier")
         )

dat = fread("large_input/api_results.csv")
sum(!dat$recipient_org_type_code %in% names(org_type_map)) / nrow(dat)
sum(!dat$donor_org_type_code %in% names(org_type_map)) / nrow(dat)
sum(!dat$donor_org_type_code %in% names(org_type_map) | !dat$recipient_org_type_code %in% names(org_type_map)) / nrow(dat)
dat$currency = toupper(dat$currency)

dat = dat %>%
  mutate(
    currency = case_match(
      currency,
      "AFG"~"AFN",
      "USS"~"USD",
      "COP"~"COU",
      "ZMK"~"ZMW",
      "FKP"~"GBP",
      .default = currency
    ),
    .keep = "all"
  )

for(unique_cc in unique(dat$currency)){
  if(!unique_cc %in% ex_rates$currency){
    message(unique_cc)
  }
}

dat = merge(dat, ex_rates, by=c("transaction_year", "currency"))

dat$value_usd = dat$value * dat$multiplier


dat_agg = dat[,.(value_usd=sum(value_usd, na.rm=T)), by=.(
  transaction_year, donor_org_type_code, recipient_org_type_code
)]
nrow(dat_agg)
dat_agg = subset(dat_agg, donor_org_type_code %in% names(org_type_map) & recipient_org_type_code %in% names(org_type_map))
dat_agg$donor_org_type = org_type_map[dat_agg$donor_org_type_code]
dat_agg$recipient_org_type = org_type_map[dat_agg$recipient_org_type_code]
dat_agg[,c("donor_org_type_code", "recipient_org_type_code")] = NULL
fwrite(dat_agg, "output/iati_org_type_aggregate_flows.csv")
