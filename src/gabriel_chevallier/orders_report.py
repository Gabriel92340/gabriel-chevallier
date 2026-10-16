import argparse
import os

import duckdb


def rapport_commandes(produit=None):
    connexion = duckdb.connect()
    connexion.execute("SET TimeZone = 'UTC'")
    bronze = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"
    return connexion.execute(
        f"""
        SELECT strftime(date, '%Y-%m') AS month, count(*) AS orders, sum(quantity) AS quantity
        FROM read_csv('{bronze}/orders.csv', strict_mode = false)
        WHERE $produit IS NULL OR product = $produit
        GROUP BY month
        ORDER BY month
        """,
        {"produit": produit},
    ).fetchall()


def main():
    parseur = argparse.ArgumentParser(prog="orders-report", description="Rapport mensuel des commandes")
    parseur.add_argument("-p", "--product", help="Filtrer les commandes sur un produit.")
    arguments = parseur.parse_args()
    for mois, commandes, quantite in rapport_commandes(arguments.product):
        print(f"{mois}\t{commandes}\t{quantite}")


if __name__ == "__main__":
    main()
