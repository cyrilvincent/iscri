import numpy as np
import matplotlib.pyplot as plt
import imageio.v2 as imageio

mat1 = np.random.rand(8,8)
mat2 = np.random.rand(8,8)
step = 100
for k in range(step + 1):
    print(k)
    mat = np.zeros((8,8))
    for row in range(8):
        for col in range(8):
            mat[row,col] = mat1[row, col] + (mat2[row, col] - mat1[row, col]) * k / step
    fig, ax = plt.subplots()
    matax = ax.matshow(mat, vmin=0, vmax=1)
    ax.set_title(f"Step {k}")
    ax.set_ylabel("Actors")
    for (i, j), z in np.ndenumerate(mat):
        ax.text(j, i, '{:0.2f}'.format(z), ha='center', va='center')
    cbar = fig.colorbar(matax)
    cbar.ax.set_ylim(0, 1)
    fig.savefig(f"images/mat{k}.png")
    plt.close(fig)
# plt.show()

with imageio.get_writer("images/animation.gif", fps=5, loop=10) as writer:
    for i in range(step):
        image = imageio.imread(f"images/mat{i}.png")
        writer.append_data(image)