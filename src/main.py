from fastapi import FastAPI,HTTPException,Path
from pydantic import BaseModel
from .connect import get_sqlalchamey_engine,read_config
from sqlalchemy import text

app=FastAPI()

#pydantic model
class customers(BaseModel):
    customer_Id:int
    name: str
    email:str
    address: str
    phone:int

#get all customers
@app.get("/customers/")
def read_all_user():
    engine = get_sqlalchamey_engine(read_config())
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection failed")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT customer_id, name, email, address, phone FROM customers"))
        rows = result.mappings().fetchall()  # 🔄 get results as dict-like mappings

    return rows
#get  all users by ID
@app.get("/customers/{id}")
def read_user(id: int = Path(..., description="The ID of the customer to retrieve")):
    engine = get_sqlalchamey_engine(read_config())
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection failed")

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT customer_id, name, email, address, phone FROM customers WHERE customer_id = :id"),
            {"id": id}
        )
        row = result.mappings().fetchone()  # 🔄 safe conversion

    if row:
        return row
    else:
        raise HTTPException(status_code=404, detail="User not found")
#create a new customer
@app.post("/customers/")
def create_customer(customer: customers):
    engine = get_sqlalchamey_engine(read_config())
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection failed")

    with engine.connect() as conn:
        conn.execute(
            text("INSERT INTO customers (customer_id, name, email, address, phone) VALUES (:id, :name, :email, :address, :phone)"),
            {
                "id": customer.customer_Id,
                "name": customer.name,
                "email": customer.email,
                "address": customer.address,
                "phone": customer.phone
            }
        )
        conn.commit()

    return {"message": "Customer created successfully"}
# Update an Existing customer Based on customer_id
@app.put("/customers/{id}")
def update_user(id: int, customer: customers):
    engine = get_sqlalchamey_engine(read_config())
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection failed")

    with engine.connect() as conn:
        conn.execute(
            text("UPDATE customers SET name = :name, email = :email, address = :address, phone = :phone WHERE customer_id = :id"),
            {
                "name": customer.name,
                "email": customer.email,
                "address": customer.address,
                "phone": customer.phone,
                "id": id
            }
        )
        conn.commit()

    return {"message": "Customer updated successfully"}

# Delete an Existing customer on customer_id
@app.delete("/customers/{id}")
def delete_user(id: int):
    engine = get_sqlalchamey_engine(read_config())
    if not engine:
        raise HTTPException(status_code=500, detail="Database connection failed")

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM customers WHERE customer_id = :id"),
            {"id": id}
        )
        conn.commit()

    return {"message": "Customer deleted successfully"}

# Define a port
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)


