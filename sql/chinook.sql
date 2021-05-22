SELECT
  c.CustomerId,
  FirstName || ' ' || LastName AS FullName,
  Company,
  Address,
  City,
  State,
  Country,
  PostalCode,
  Phone,
  Fax,
  Email,
  InvoiceId,
  strftime('%Y-%m-%d', InvoiceDate) AS InvoiceDate,
  BillingAddress,
  BillingCity,
  BillingState,
  BillingCountry,
  BillingPostalCode,
  Total
FROM customers c
LEFT JOIN invoices i ON c.CustomerId = i.CustomerId