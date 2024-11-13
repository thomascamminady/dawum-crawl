from dawum_crawl.crawler import Crawler
from dawum_crawl.plotter import Plotter

crawler = Crawler()
plotter = Plotter()

crawler.save()
df_pivot = crawler.df_pivot()
chart = plotter.plot(df_pivot)
chart.save("data/Polling.png", scale_factor=2.0)
