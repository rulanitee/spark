package tendai.spark.test

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, hash}

/**
  * Created by dev on 7/12/18.
  */
class SparkTestLoan extends SparkTestBase{

  "testing student hash" in {

    val f = SparkTestFileUtil.getTestResourceAsFile("/loandetails.parquet")

    println(s"testing: ${f.getName}")

    //sparkSession.sql("CREATE DATABASE IF NOT EXISTS hs_model")

    val df = sparkSession.read.format("parquet").load(f.toString)

    //println(s"schema: ${df.schema.treeString}")

    //df.count

    val dfHashed = df.select(col("ActiveRPB"),
      col("Agency"),
      col("AgencyCode"),
      col("AnnualMIPRateAtIssuance"),
      col("AtIssFctrDt"),
      col("BuyDownStatusFlagAtIssuance"),
      col("Coupon"),
      col("CPR1"),
      col("CPR3"),
      col("CPR6"),
      col("CPR12"),
      col("CPR24"),
      col("CPRLife"),
      col("CS"),
      col("CSV"),
      col("CurrMonLiquidationFlag"),
      col("CurrRPB"),
      col("CUSIP"),
      col("DelMonths"),
      col("DocTypeAssetFlag"),
      col("DocTypeEmplFlag"),
      col("DocTypeIncomeFlag"),
      col("DPAFlag"),
      col("DTI"),
      col("FctrDt"),
      col("FirstPaymtDt"),
      col("FirstTimeHomeBuyerFlag"),
      col("GovtAgy"),
      col("GrossMarginAtIssuance"),
      col("IsActive"),
      col("IssuerCategory"),
      col("LoanAge"),
      col("LoanPurpose"),
      col("LoanSeqNum"),
      col("MortgageNoteRate"),
      col("MSA"),
      col("NumBorrowers"),
      col("NumUnits"),
      col("OccupancyType"),
      col("OccupancyTypeAtIssuance"),
      col("OrigCombLTV"),
      col("OriginatorAtIssuance"),
      col("OrigLoanAmt"),
      col("OrigLTV"),
      col("OrigNoteRate"),
      col("OrigTerm"),
      col("PctMtgIns"),
      col("PoolIssDt"),
      col("PoolIssRPB"),
      col("PoolNr"),
      col("Prefix"),
      col("PrefixId"),
      col("Product"),
      col("PropertyType"),
      col("RefiType"),
      col("RemovalReason"),
      col("RemTerm"),
      col("RPBCS"),
      col("RPBDTI"),
      col("RPBLoanAge"),
      col("RPBMortgageNoteRate"),
      col("RPBOrigLTV"),
      col("SecMnem"),
      col("Seller"),
      col("Servicer"),
      col("ServicerAtIssuance"),
      col("State"),
      col("Status"),
      col("TPO"),
      col("UpdDt"),
      col("UpdSrc"),
      col("UpfrontMIPRateAtIssuance"),
      col("WeightedCPR1"),
      col("WeightedCS"),
      col("WeightedDTI"),
      col("WeightedLoanAge"),
      col("WeightedMortgageNoteRate"),
      col("WeightedOrigLTV"),
      col("annotations"),
      col("LoanLevelViewId"),
      col("RPBCPR1")).withColumn("hash", hash(df.columns.map(col): _*))

    dfHashed.select(col("LoanLevelViewId"), col("hash")).show(false)

    println(s"schema: ${dfHashed.schema.treeString}")

    /*val count = df.count()

    println(s"Count: $count")

    sparkSession.catalog.setCurrentDatabase("hs_model")

    if (sparkSession.catalog.tableExists("loandetails")){

      val dropTable = "DROP TABLE IF EXISTS hs_model.loandetails"

      sparkSession.sql(dropTable)

      if (sparkSession.catalog.tableExists("loandetails")){

        throw new RuntimeException("drop failed")

      }

    }

    df.write.mode(SaveMode.Overwrite).saveAsTable("hs_model.loandetails")*/

    assert(20 == 20 , s"The age should be 20 ...")

  }


}
