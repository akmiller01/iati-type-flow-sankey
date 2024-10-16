list.of.packages <- c("rstudioapi", "data.table", "networkD3", "htmlwidgets")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only=T)

wd <- dirname(getActiveDocumentContext()$path) 
setwd(wd)
setwd("../")

dat = fread("output/iati_org_type_aggregate_flows.csv")

dat = subset(dat, transaction_year==2023)
dat$value_usd_billions = dat$value_usd / 1e9

dat$donor_org_type = paste0("Donor: ", dat$donor_org_type)
dat$recipient_org_type = paste0("Recipient: ", dat$recipient_org_type)
nodes = data.frame(name=c(dat$donor_org_type, dat$recipient_org_type))
nodes = unique(nodes)

link_list = list()
for(i in 1:nrow(dat)){
  from = dat[i, "donor_org_type"][[1]]
  from_index = which(nodes$name==from) - 1
  to = dat[i, "recipient_org_type"][[1]]
  to_index = which(nodes$name==to) - 1
  value_usd_billions = dat[i, "value_usd_billions"][[1]]
  link_df = data.frame(source=from_index, target=to_index, value=value_usd_billions)
  link_list[[i]] = link_df
}
links = rbindlist(link_list)
nodes = data.frame(nodes)
links = data.frame(links)

# Thus we can plot it
p <- sankeyNetwork(Links = links, Nodes = nodes, Source = "source",
                   Target = "target", Value = "value", NodeID = "name",
                   units = "$ billion", fontSize = 12, nodeWidth = 30)
p

# save the widget
saveWidget(p, file="output/sankey.html")
