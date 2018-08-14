import matplotlib

def style():
    matplotlib.rcParams["font.size"] = 16
    matplotlib.rcParams["figure.figsize"] = (14, 7)
    matplotlib.rcParams["axes.grid"] = True
    
def M_str(x, pos):
    n = int(round(x / 1000000))
    return "{n} M".format(n=n)

pct_fmt = matplotlib.ticker.PercentFormatter(xmax=1)
M_fmt = matplotlib.ticker.FuncFormatter(M_str)
comma_fmt = matplotlib.ticker.StrMethodFormatter("{x:,.0f}")