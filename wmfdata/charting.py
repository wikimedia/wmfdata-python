import matplotlib as mpl
from matplotlib import ticker

mpl_style = {
    "font.size": 16,
    "figure.figsize": (14, 7),
    "axes.grid": True,
    "axes.autolimit_mode": "data",
    "axes.xmargin": 0,
    "axes.ymargin": 0
}

def set_mpl_style():
    mpl.style.use(mpl_style)
    
def M_str(x, pos):
    n = int(round(x / 1000000))
    return "{n} M".format(n=n)

pct_fmt = ticker.PercentFormatter(xmax=1)
M_fmt = ticker.FuncFormatter(M_str)
comma_fmt = ticker.StrMethodFormatter("{x:,.0f}")