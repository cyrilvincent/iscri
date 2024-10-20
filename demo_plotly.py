import plotly.graph_objects as go
import numpy as np

# Create figure
fig = go.Figure()

plot_trace = []
# Add traces, one for each slider step
for step in np.arange(0, 5, 0.1):
    #fig.add_trace(
    plot_trace.append(
        go.Scatter(
            visible=False,
            line=dict(color="#00CED1", width=6),
            name="𝜈 = " + str(step),
            x=np.arange(0, 10, 0.01),
            y=np.sin(step * np.arange(0, 10, 0.01))))

# Make 10th trace visible
# fig.data[10].visible = True
plot_trace[-1].visible = True
for t in plot_trace:
    fig.add_trace(t)
fig.frames = [go.Frame(data=x) for x in plot_trace]

# Create and add slider
steps = []
for i in range(len(fig.data)):
    step = dict(
        method="update",
        args=[{"visible": [False] * len(fig.data)},
              {"title": "Slider switched to step: " + str(i)}],  # layout attribute
    )
    step["args"][0]["visible"][i] = True  # Toggle i'th trace to "visible"
    steps.append(step)

sliders = [dict(
    # active=10,
    currentvalue={"prefix": "Frequency: "},
    pad={"t": 50},
    steps=steps
)]


fig.update_layout(
    sliders=sliders,
    updatemenus=[dict(
            type="buttons",
            buttons=[dict(label="Play",
                          method="update",
                          args=[{"sliders": sliders},
                          ])])],
)
# fig.write_html("data/out.html")
fig.show()
