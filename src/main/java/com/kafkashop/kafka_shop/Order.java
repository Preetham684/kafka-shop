package com.kafkashop.kafka_shop;

public class Order {

	private int orderId;
    private String product;
    private int quantity;
    private double price;
    private String customerEmail;
    
    public Order() {}
    
    public Order(
            int orderId,
            String product,
            int quantity,
            double price,
            String customerEmail) {

        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.customerEmail = customerEmail;
    }
    
    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
    
    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

}
