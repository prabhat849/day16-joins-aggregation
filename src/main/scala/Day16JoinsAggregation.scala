
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16JoinsAggregation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16-Joins-Aggregation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("========================================")
    println("DAY 16 - SPARK JOINS AND AGGREGATION")
    println("========================================")

    // 1. Employee dataset: 15 employees
    val employees = Seq(
      (1, "Prabhat", 25000.0, "D01"),
      (2, "Rahul", 45000.0, "D02"),
      (3, "Amit", 75000.0, "D01"),
      (4, "Sneha", 120000.0, "D03"),
      (5, "Priya", 60000.0, "D02"),
      (6, "Arjun", 28000.0, "D04"),
      (7, "Neha", 55000.0, "D02"),
      (8, "Karan", 95000.0, "D01"),
      (9, "Anjali", 35000.0, "D03"),
      (10, "Rohit", 85000.0, "D01"),
      (11, "Pooja", 150000.0, "D04"),
      (12, "Vikram", 18000.0, "D05"),
      (13, "Manish", 72000.0, "D02"),
      (14, "Kavita", 68000.0, "D05"),
      (15, "Suresh", 50000.0, "D03")
    ).toDF("employee_id", "employee_name", "salary", "dept_id")

    println("\n===== EMPLOYEE DATA: 15 RECORDS =====")
    employees.show(15, false)

    // 2. Department dataset: 5 departments
    val departments = Seq(
      ("D01", "Engineering"),
      ("D02", "Data Engineering"),
      ("D03", "Human Resources"),
      ("D04", "Finance"),
      ("D05", "Marketing")
    ).toDF("dept_id", "dept_name")

    println("\n===== DEPARTMENT DATA: 5 RECORDS =====")
    departments.show(false)

    // 3. Inner join: match employees with departments
    val joinedData = employees.join(
      departments,
      Seq("dept_id"),
      "inner"
    )

    println("\n===== INNER JOIN: EMPLOYEES AND DEPARTMENTS =====")
    joinedData
      .select("employee_id", "employee_name", "salary", "dept_name")
      .orderBy("employee_id")
      .show(15, false)

    // 4. Aggregation: employee count and average salary
    val departmentSummary = joinedData
      .groupBy("dept_name")
      .agg(
        count("employee_id").as("employee_count"),
        round(avg("salary"), 2).as("average_salary"),
        sum("salary").as("total_salary"),
        max("salary").as("maximum_salary"),
        min("salary").as("minimum_salary")
      )
      .orderBy("dept_name")

    println("\n===== DEPARTMENT-WISE AGGREGATION =====")
    departmentSummary.show(false)

    // 5. Filter departments with average salary >= 50000
    println("\n===== DEPARTMENTS WITH AVERAGE SALARY >= 50000 =====")

    departmentSummary
      .filter(col("average_salary") >= 50000)
      .show(false)

    // 6. Left join example
    println("\n===== LEFT JOIN RESULT COUNT =====")

    val leftJoinData = employees.join(
      departments,
      Seq("dept_id"),
      "left"
    )

    println(s"Employee records before join: ${employees.count()}")
    println(s"Employee records after left join: ${leftJoinData.count()}")

    // 7. Concepts summary
    println("\n===== DAY 16 CONCEPTS =====")
    println("Inner Join: Returns matching rows from both datasets.")
    println("Left Join: Keeps every row from the left dataset.")
    println("groupBy: Groups rows using one or more columns.")
    println("agg: Calculates count, average, sum, max and min.")
    println("filter: Selects rows matching a condition.")

    println("\nDAY 16 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
