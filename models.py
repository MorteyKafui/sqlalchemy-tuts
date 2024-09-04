from typing import Optional

from sqlalchemy import Column, ForeignKey, String, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Model

ProductCountry = Table(
    "products_countries",
    Model.metadata,
    Column("product_id", ForeignKey("products.id"), primary_key=True, nullable=False),
    Column("country_id", ForeignKey("countries.id"), primary_key=True, nullable=False),
)


class Product(Model):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64), index=True, unique=True)
    manufacturer_id: Mapped[int] = mapped_column(ForeignKey("manufacturers.id"), index=True)
    manufacturer: Mapped["Manufacturer"] = relationship(back_populates="products", lazy="joined")
    countries: Mapped[list["Country"]] = relationship(secondary=ProductCountry, back_populates="products")
    year: Mapped[int] = mapped_column(index=True)
    country: Mapped[Optional[str]] = mapped_column(String(32))
    cpu: Mapped[Optional[str]] = mapped_column(String(32))

    def __repr__(self):
        return f"Product({self.id}, '{self.name}') "


class Manufacturer(Model):
    __tablename__ = "manufacturers"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64), index=True, unique=True)
    products: Mapped[list["Product"]] = relationship(back_populates="manufacturer", cascade="all,delete-orphan")

    def __repr__(self):
        return f"Manufacturer({self.id}, '{self.name}')"


class Country(Model):
    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), index=True, unique=True)
    products: Mapped[list["Product"]] = relationship(secondary=ProductCountry, back_populates="countries")

    def __repr__(self):
        return f"Country({self.id},'{self.name})"
