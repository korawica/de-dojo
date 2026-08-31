# Data Modeling

This section will cover the data modeling from the Bronze zone to the Silver zone.

For commonly data modeling that will exist in the Silver zone:

**Transaction**:

    Data keeps the state of full transaction record with all the details from various of master data,
    such as order, sales, inventory, etc.

**Pure Transaction**:

    Data keeps the full transaction record with all the details from various of master data,
    such as pos-log, inventory-movement, etc.

**Master**:

    Data keeps only the noun part of the transaction such as customer, product, store, etc.

**Snapshot**:

    Data keeps the full state of the transaction at a specific point in time, such
    as end of day, end of month, etc.

**State**:

    Data keeps the state of the transaction at a specific point in time, such as
    end of day, end of month, etc., but only with a subset of the details such as
    inventory-balance, etc.

**Period**:

    Data keeps the period state of the transaction, such as installment payment, etc.

---
