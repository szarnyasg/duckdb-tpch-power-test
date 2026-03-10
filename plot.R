run1 <- readr::read_tsv("old.tsv")
run1$run <- '1.4.4'
run2 <- readr::read_tsv("new.tsv")
run2$run <- '1.5.0'
# run3 <- readr::read_tsv("log-sf100-2024-08-21T19:12:27.119128+00:00.tsv")
# run3$run <- 'main-1'
# run4 <- readr::read_tsv("log-sf300-2024-09-03T11:59:03.414852+00:00.tsv")
# run4$run <- 'sf300'

runs <- rbind(run1, run2)#, run3, run4)

library(ggplot2)
pdf("cpu.pdf", height=5, width=15)
ggplot(runs, aes(x=time_offset, y=cpu_percent, colour=run)) + geom_line()
dev.off()

library(ggplot2)
pdf("io.pdf", height=5, width=15)
ggplot(runs, aes(x=time_offset, y=write_bytes, colour=run)) + geom_line()
dev.off()