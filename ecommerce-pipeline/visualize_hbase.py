"""
TP5 - Visualisation des agregats HBase
======================================
Lit la table 'sales' depuis HBase (via hbase shell dans Docker) et genere
quatre graphiques :
  1. Chiffre d'affaires par categorie (barres)
  2. Nombre d'achats par categorie (barres)
  3. CA + tendance (barres + ligne)
  4. Radar chart (toile d'araignee) toutes metriques

Usage :
    python3 visualize_hbase.py
    # Les graphiques sont sauvegardes en PNG dans le repertoire courant.

Prerequis :
    pip install matplotlib numpy
"""

import subprocess
import json
import re
import os
import sys

import matplotlib
matplotlib.use('Agg')          # mode non-interactif (pas d'ecran requis)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np


# ---------------------------------------------------------------------------
# 1. Lecture des donnees HBase via hbase shell
# ---------------------------------------------------------------------------

HBASE_CMD = """
echo "scan 'sales'" | docker exec -i hadoop-master hbase shell -n 2>/dev/null
"""

def read_hbase_sales():
    """
    Lit la table 'sales' et retourne un dict :
    { 'Electronique': {'total_amount': 2012.51, 'purchase_count': 8}, ... }
    """
    try:
        result = subprocess.run(
            HBASE_CMD, shell=True, capture_output=True, text=True, timeout=60
        )
        raw = result.stdout
    except Exception as e:
        print(f"Erreur lors de la lecture HBase : {e}")
        return _demo_data()

    data = {}
    for line in raw.splitlines():
        # Exemple : " Electronique column=stats:total_amount, ..., value=2012.51"
        m_row = re.match(r'^\s+(\S+)\s+column=(\S+),.*value=(.+)$', line)
        if not m_row:
            continue
        category = m_row.group(1)
        column   = m_row.group(2)   # ex. stats:total_amount
        value    = m_row.group(3).strip()

        if category not in data:
            data[category] = {}

        if column == 'stats:total_amount_plain':
            try:
                data[category]['total_amount'] = float(value)
            except ValueError:
                pass
        elif column == 'stats:purchase_count':
            try:
                data[category]['purchase_count'] = int(value)
            except ValueError:
                pass

    if not data:
        print("Aucune donnee lue dans HBase, utilisation des donnees de demonstration.")
        return _demo_data()

    return data


def _demo_data():
    """Donnees de demonstration si HBase n'est pas accessible."""
    return {
        'Electronique': {'total_amount': 2012.51, 'purchase_count': 8},
        'Livres':       {'total_amount': 2955.90, 'purchase_count': 9},
        'Maison':       {'total_amount': 1466.53, 'purchase_count': 8},
        'Sport':        {'total_amount': 1514.27, 'purchase_count': 7},
        'Vetements':    {'total_amount': 1503.43, 'purchase_count': 8},
    }


# ---------------------------------------------------------------------------
# 2. Preparation des donnees
# ---------------------------------------------------------------------------

def prepare(data):
    categories = sorted(data.keys())
    ca_values  = [data[c].get('total_amount',  0.0) for c in categories]
    nb_values  = [data[c].get('purchase_count', 0)  for c in categories]
    return categories, ca_values, nb_values


COLORS = ['#4C72B0', '#DD8452', '#55A868', '#C44E52', '#8172B2']


# ---------------------------------------------------------------------------
# 3. Graphiques
# ---------------------------------------------------------------------------

def plot_ca_bar(categories, ca_values, output='chart_ca.png'):
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(categories, ca_values, color=COLORS, edgecolor='white', linewidth=1.2)
    ax.set_title("Chiffre d'affaires par categorie (fenetre courante)", fontsize=14, pad=12)
    ax.set_xlabel("Categorie", fontsize=12)
    ax.set_ylabel("CA cumule (EUR)", fontsize=12)
    ax.yaxis.grid(True, linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)
    for bar, val in zip(bars, ca_values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f'{val:.2f}', ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    plt.savefig(output, dpi=150)
    plt.close()
    print(f"  -> {output}")


def plot_purchases_bar(categories, nb_values, output='chart_purchases.png'):
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(categories, nb_values, color=COLORS, edgecolor='white', linewidth=1.2)
    ax.set_title("Nombre d'achats par categorie (fenetre courante)", fontsize=14, pad=12)
    ax.set_xlabel("Categorie", fontsize=12)
    ax.set_ylabel("Nombre d'achats", fontsize=12)
    ax.yaxis.grid(True, linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)
    for bar, val in zip(bars, nb_values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1,
                str(val), ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output, dpi=150)
    plt.close()
    print(f"  -> {output}")


def plot_combined(categories, ca_values, nb_values, output='chart_combined.png'):
    """Double axe Y : CA (barres) + nombre d'achats (ligne)."""
    fig, ax1 = plt.subplots(figsize=(11, 6))
    x = np.arange(len(categories))
    bars = ax1.bar(x, ca_values, color=COLORS, alpha=0.85, edgecolor='white', linewidth=1.2)
    ax1.set_xlabel("Categorie", fontsize=12)
    ax1.set_ylabel("CA cumule (EUR)", color='#4C72B0', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='#4C72B0')
    ax1.set_xticks(x)
    ax1.set_xticklabels(categories)

    ax2 = ax1.twinx()
    ax2.plot(x, nb_values, color='#C44E52', marker='o', linewidth=2.5,
             markersize=8, label="Nb achats")
    ax2.set_ylabel("Nombre d'achats", color='#C44E52', fontsize=12)
    ax2.tick_params(axis='y', labelcolor='#C44E52')

    ax1.set_title("CA et nombre d'achats par categorie", fontsize=14, pad=12)
    ax1.yaxis.grid(True, linestyle='--', alpha=0.3)

    patch_ca  = mpatches.Patch(color='#4C72B0', alpha=0.85, label='CA (EUR)')
    patch_nb  = mpatches.Patch(color='#C44E52', label="Nb achats")
    ax1.legend(handles=[patch_ca, patch_nb], loc='upper right')

    plt.tight_layout()
    plt.savefig(output, dpi=150)
    plt.close()
    print(f"  -> {output}")


def plot_radar(categories, ca_values, nb_values, output='chart_radar.png'):
    """Radar chart (spider/web) : normalise CA et nb achats sur [0,1]."""
    N = len(categories)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]  # fermer le polygone

    # Normalisation [0, 1]
    max_ca = max(ca_values) or 1
    max_nb = max(nb_values) or 1
    ca_norm = [v / max_ca for v in ca_values] + [ca_values[0] / max_ca]
    nb_norm = [v / max_nb for v in nb_values] + [nb_values[0] / max_nb]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

    ax.plot(angles, ca_norm, color='#4C72B0', linewidth=2, label='CA (normalise)')
    ax.fill(angles, ca_norm, color='#4C72B0', alpha=0.25)

    ax.plot(angles, nb_norm, color='#DD8452', linewidth=2, label='Nb achats (normalise)')
    ax.fill(angles, nb_norm, color='#DD8452', alpha=0.25)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=11)
    ax.set_yticklabels([])
    ax.set_title("Radar : CA vs Nombre d'achats par categorie\n(valeurs normalisees)",
                 fontsize=13, pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))

    plt.tight_layout()
    plt.savefig(output, dpi=150)
    plt.close()
    print(f"  -> {output}")


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

def main():
    print("=== Lecture des donnees HBase (table 'sales') ===")
    data = read_hbase_sales()

    print(f"\nDonnees lues ({len(data)} categories) :")
    for cat, vals in sorted(data.items()):
        print(f"  {cat:15s} : CA={vals.get('total_amount',0):.2f} EUR"
              f"  |  achats={vals.get('purchase_count',0)}")

    categories, ca_values, nb_values = prepare(data)

    print("\n=== Generation des graphiques ===")
    plot_ca_bar(categories, ca_values)
    plot_purchases_bar(categories, nb_values)
    plot_combined(categories, ca_values, nb_values)
    plot_radar(categories, ca_values, nb_values)

    print("\nDone. 4 fichiers PNG generes dans le repertoire courant.")


if __name__ == '__main__':
    main()
