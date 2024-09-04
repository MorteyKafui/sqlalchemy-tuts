import csv
import select
from datetime import datetime

from sqlalchemy import delete

from db import Session
from models import Country, Customer, Manufacturer, Order, OrderItem, Product, ProductCountry


def main():  # noqa: C901
    with Session() as session:
        with session.begin():
            session.execute(delete(ProductCountry))
            session.execute(delete(Product))
            session.execute(delete(Manufacturer))
            session.execute(delete(Country))
            session.execute(delete(OrderItem))
            session.execute(delete(Order))
            session.execute(delete(Customer))

    with Session() as session:
        with session.begin():
            with open("products.csv") as f:
                reader = csv.DictReader(f)
                all_manufacturers = {}
                all_countries = {}
                all_customers = {}
                all_products = {}

                for row in reader:
                    row["year"] = int(row["year"])

                    manufacturer = row.pop("manufacturer")
                    countries = row.pop("country").split("/")
                    p = Product(**row)

                    if manufacturer not in all_manufacturers:
                        m = Manufacturer(name=manufacturer)
                        session.add(m)
                        all_manufacturers[manufacturer] = m
                    all_manufacturers[manufacturer].products.append(p)

                    for country in countries:
                        if country not in all_countries:
                            c = Country(name=country)
                            session.add(c)
                            all_countries[country] = c
                        all_countries[country].products.append(p)

                    if row["name"] not in all_customers:
                        c = Customer(name=row["name"], address=row.get("address", None), phone=row.get("phone", None))
                        all_customers[row["name"]] = c
                    o = Order(timestamp=datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S"))

                    all_customers[row["name"]].orders.add(o)

                    product = all_products.get(row["product1"])
                    if product is None:
                        product = session.scalar(select(Product).where(Product.name == row["product1"]))
                        all_products[row["product1"]] = product
                    o.order_items.append(
                        OrderItem(product=product, unit_price=float(row["unit_price1"]), quantity=int(row["quantity1"]))
                    )

                    if row["product2"]:
                        product = all_products.get(row["product2"])
                        if product is None:
                            product = session.scalar(select(Product).where(Product.name == row["product2"]))
                            all_products[row["product2"]] = product
                        o.order_items.append(
                            OrderItem(
                                product=product, unit_price=float(row["unit_price2"]), quantity=int(row["quantity2"])
                            )
                        )
                    if row["product3"]:
                        product = all_products.get(row["product3"])
                        if product is None:
                            product = session.scalar(select(Product).where(Product.name == row["product3"]))
                            all_products[row["product3"]] = product
                        o.order_items.append(
                            OrderItem(
                                product=product, unit_price=float(row["unit_price3"]), quantity=int(row["quantity3"])
                            )
                        )


if __name__ == "__main__":
    main()
