from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("transformations_and_actions").getOrCreate()

df = spark.read.csv("employee_data.csv", header=True, inferSchema=True)
# df.show(5)

df_sales = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
# df.show(5)


# ==============================================|| Transformations ||=========================================

# Select 'name' and 'salary' columns from the employee DataFrame
df_selected = df.select("name", "salary")
df_selected.show()

# Filter employees with salary greater than 60,000
df_filtered = df.filter(df.salary > 60000)
df_filtered.show()

# Group employees by department and calculate average salary
df_grouped = df.groupBy("department").avg("salary")
df_grouped.show()

# Add a new column 'bonus' which is 10% of the salary
df_with_bonus = df.withColumn("bonus", df.salary * 0.1)
df_with_bonus.show()

# Join employee and sales DataFrames on 'id' (assumed common column)
df_joined = df.join(df_sales, df.id == df_sales.customer_id, "inner")
df_joined.show()

# Order employees by age in ascending order
df_ordered = df.orderBy("age")
df_ordered.show()

# Select distinct departments from the employee DataFrame
df_distinct = df.select("department").distinct()
df_distinct.show()


# ==============================================|| Actions ||=========================================
# Collect all rows from the DataFrame as a list
data_collected = df.collect()
print(data_collected)

# Count the number of employees
employee_count = df.count()
print(f"Number of employees: {employee_count}")

# Show the first 5 rows of the employee DataFrame
df.show(5)

# Take the first 3 rows from the employee DataFrame
first_three_rows = df.take(3)
print(first_three_rows)

# Get the first row of the DataFrame
first_row = df.first()
print(first_row)

# Write the DataFrame to a CSV file
df.write.csv("employees.csv", header=True)


# Print each employee's name
def print_employee_name(row):
    print(row.name)


df.foreach(print_employee_name)
