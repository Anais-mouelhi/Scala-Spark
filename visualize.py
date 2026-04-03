"""
Visualisation des resultats du pipeline capteurs stockes dans HBase.
Lit la table 'city_stats' via Docker + HBase shell, genere des graphes matplotlib.
"""

import subprocess
import re
import os
import matplotlib
matplotlib.use('Agg')  # Backend non-interactif pour generer des PNG
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------------------------------------------------------------------------
# 1. Lecture des donnees depuis HBase (via docker exec)
# ---------------------------------------------------------------------------

def read_hbase_table(table_name):
    """Execute un scan HBase et retourne le texte brut."""
    cmd = [
        "docker", "exec", "hadoop-master",
        "bash", "-c",
        f"source /etc/profile && echo \"scan '{table_name}'\" | "
        f"/usr/local/hbase/bin/hbase shell -n 2>/dev/null"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.stdout


def parse_city_stats(raw):
    """
    Parse la sortie du scan HBase et retourne un dict :
    { city -> { 'avg_temperature': float, 'avg_humidity': float, 'nb_mesures': int } }
    """
    data = {}
    pattern = re.compile(
        r'^\s+(\w+)\s+column=stats:(\w+),\s+timestamp=.+?,\s+value=(.+)$'
    )
    for line in raw.splitlines():
        m = pattern.match(line)
        if m:
            city, col, val = m.group(1), m.group(2), m.group(3).strip()
            if city not in data:
                data[city] = {}
            try:
                data[city][col] = float(val) if col != 'nb_mesures' else int(val)
            except ValueError:
                pass
    return data


# ---------------------------------------------------------------------------
# 2. Generation des graphes
# ---------------------------------------------------------------------------

COLORS_TEMP = ['#e74c3c', '#e67e22', '#e74c3c', '#c0392b', '#3498db']
COLORS_HUM  = ['#2980b9', '#27ae60', '#1abc9c', '#16a085', '#8e44ad']
OUTPUT_DIR  = os.path.dirname(os.path.abspath(__file__))


def plot_temperature(data, output_dir):
    cities = list(data.keys())
    temps  = [data[c]['avg_temperature'] for c in cities]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(cities, temps, color=COLORS_TEMP[:len(cities)],
                  edgecolor='white', linewidth=1.2, width=0.55)

    # Valeurs sur les barres
    for bar, val in zip(bars, temps):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                f'{val:.1f} C', ha='center', va='bottom',
                fontsize=11, fontweight='bold', color='#2c3e50')

    ax.set_title('Temperature moyenne par ville\n(source : HBase > city_stats)',
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Ville', fontsize=12)
    ax.set_ylabel('Temperature moyenne (C)', fontsize=12)
    ax.set_ylim(0, max(temps) * 1.25)
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('#ffffff')
    ax.grid(axis='y', linestyle='--', alpha=0.6)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    out = os.path.join(output_dir, 'chart_temperature.png')
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f'  Graphe sauvegarde : {out}')


def plot_humidity(data, output_dir):
    cities = list(data.keys())
    hums   = [data[c]['avg_humidity'] for c in cities]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(cities, hums, color=COLORS_HUM[:len(cities)],
                  edgecolor='white', linewidth=1.2, width=0.55)

    for bar, val in zip(bars, hums):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                f'{val:.1f}%', ha='center', va='bottom',
                fontsize=11, fontweight='bold', color='#2c3e50')

    ax.set_title('Humidite moyenne par ville\n(source : HBase > city_stats)',
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Ville', fontsize=12)
    ax.set_ylabel('Humidite moyenne (%)', fontsize=12)
    ax.set_ylim(0, max(hums) * 1.25)
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('#ffffff')
    ax.grid(axis='y', linestyle='--', alpha=0.6)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    out = os.path.join(output_dir, 'chart_humidity.png')
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f'  Graphe sauvegarde : {out}')


def plot_combined(data, output_dir):
    """Graphe double-axe : temperature (barres) + humidite (ligne)."""
    cities = list(data.keys())
    temps  = [data[c]['avg_temperature'] for c in cities]
    hums   = [data[c]['avg_humidity'] for c in cities]
    x      = np.arange(len(cities))

    fig, ax1 = plt.subplots(figsize=(10, 6))

    bars = ax1.bar(x, temps, width=0.45, color='#e74c3c', alpha=0.85,
                   label='Temp. moy. (C)', edgecolor='white')
    ax1.set_xlabel('Ville', fontsize=12)
    ax1.set_ylabel('Temperature (C)', color='#e74c3c', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#e74c3c')
    ax1.set_xticks(x)
    ax1.set_xticklabels(cities, fontsize=11)
    ax1.set_ylim(0, max(temps) * 1.35)

    ax2 = ax1.twinx()
    ax2.plot(x, hums, 'o-', color='#2980b9', linewidth=2.5,
             markersize=8, label='Humidite moy. (%)')
    ax2.set_ylabel('Humidite (%)', color='#2980b9', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#2980b9')
    ax2.set_ylim(0, max(hums) * 1.35)

    ax1.set_title('Donnees capteurs par ville - Temperature & Humidite\n(Pipeline : Kafka -> Spark -> HBase -> Visualisation)',
                  fontsize=13, fontweight='bold', pad=15)

    patch1 = mpatches.Patch(color='#e74c3c', alpha=0.85, label='Temp. moy. (C)')
    line1  = plt.Line2D([0], [0], color='#2980b9', linewidth=2.5,
                        marker='o', markersize=8, label='Humidite moy. (%)')
    ax1.legend(handles=[patch1, line1], loc='upper right', fontsize=10)

    ax1.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('#ffffff')
    ax1.grid(axis='y', linestyle='--', alpha=0.4)
    ax1.spines['top'].set_visible(False)

    out = os.path.join(output_dir, 'chart_combined.png')
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f'  Graphe sauvegarde : {out}')


def plot_radar(data, output_dir):
    """Graphe radar par ville (temperature normalisee + humidite normalisee)."""
    cities = list(data.keys())
    temps  = np.array([data[c]['avg_temperature'] for c in cities])
    hums   = np.array([data[c]['avg_humidity']    for c in cities])

    # Normalisation 0-1
    temps_n = (temps - temps.min()) / (temps.max() - temps.min())
    hums_n  = (hums  - hums.min())  / (hums.max()  - hums.min())

    angles  = np.linspace(0, 2 * np.pi, len(cities), endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw=dict(polar=True))

    def draw(values, color, label):
        v = values.tolist() + values[:1].tolist()
        ax.plot(angles, v, 'o-', linewidth=2, color=color)
        ax.fill(angles, v, alpha=0.2, color=color, label=label)

    draw(temps_n, '#e74c3c', 'Temperature (norm.)')
    draw(hums_n,  '#2980b9', 'Humidite (norm.)')

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(cities, fontsize=11)
    ax.set_yticklabels([])
    ax.set_title('Profil climatique par ville\n(valeurs normalisees 0-1)',
                 size=13, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), fontsize=10)

    out = os.path.join(output_dir, 'chart_radar.png')
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f'  Graphe sauvegarde : {out}')


# ---------------------------------------------------------------------------
# 3. Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    print('=== Visualisation des resultats HBase ===\n')

    print('Lecture de la table city_stats depuis HBase...')
    raw  = read_hbase_table('city_stats')
    data = parse_city_stats(raw)

    if not data:
        print('ERREUR : aucune donnee lue. Verifiez que HBase est lance et que city_stats existe.')
        exit(1)

    print(f'  {len(data)} villes chargees : {", ".join(data.keys())}\n')
    print('Donnees chargees :')
    for city, vals in data.items():
        print(f'  {city:12s} | temp={vals.get("avg_temperature","?"):5}C | '
              f'hum={vals.get("avg_humidity","?"):5}% | '
              f'n={vals.get("nb_mesures","?")}')

    print('\nGeneration des graphes...')
    plot_temperature(data, OUTPUT_DIR)
    plot_humidity(data, OUTPUT_DIR)
    plot_combined(data, OUTPUT_DIR)
    plot_radar(data, OUTPUT_DIR)

    print('\n=== Graphes generes dans', OUTPUT_DIR, '===')
    print('  chart_temperature.png')
    print('  chart_humidity.png')
    print('  chart_combined.png')
    print('  chart_radar.png')
