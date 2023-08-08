# Databricks notebook source

from datetime import timedelta, datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
#from pyspark.sql.functions import sha2, concat_ws
spark.sql("use catalog development1")
spark.sql("use schema gl1_s")

batch_start_time = datetime.utcnow()


# COMMAND ----------


fields_technical = [
    StructField("dl_iscurrent", BooleanType(), True),
    StructField("dl_recordstartdateutc", TimestampType(), True),
    StructField("dl_recordenddateutc", TimestampType(), True),
    StructField("dl_update_tm", TimestampType(), True),
    StructField("dl_rowhash", StringType(), True)
]

fields_target = [

 StructField("client", StringType(), True,   
    {
   "heading" : "Cl.", 
   "long_label" : "Client", 
   "medium_label" : "Client", 
   "conversion_routine" : "" 
    } ),
 StructField("ledger", StringType(), True,   
    {
   "heading" : "Ld", 
   "long_label" : "Ledger", 
   "medium_label" : "Ledger", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("company_code", StringType(), True,   
    {
   "heading" : "CoCd", 
   "long_label" : "Company Code", 
   "medium_label" : "Company Code", 
   "conversion_routine" : "" 
    } ),
 StructField("fiscal_year", IntegerType(), True,   
    {
   "heading" : "Year", 
   "long_label" : "Fiscal Year", 
   "medium_label" : "Fiscal Year", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("document_number", StringType(), True,   
    {
   "heading" : "DocumentNo", 
   "long_label" : "Document Number", 
   "medium_label" : "Document Number", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("posting_item", StringType(), True,   
    {
   "heading" : "PsItm", 
   "long_label" : "Posting Item", 
   "medium_label" : "Posting Item", 
   "conversion_routine" : "" 
    } ),
 StructField("gl_fiscal_year", IntegerType(), True,   
    {
   "heading" : "GLFY", 
   "long_label" : "G/L Fiscal Year", 
   "medium_label" : "G/L Fiscal Year", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("ledger_spec_docno", StringType(), True,   
    {
   "heading" : "DocNoLD", 
   "long_label" : "Ledger specific Document Number", 
   "medium_label" : "Ledger spec. DocNo", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("record_type", StringType(), True,   
    {
   "heading" : "R", 
   "long_label" : "Record Type", 
   "medium_label" : "Record Type", 
   "conversion_routine" : "" 
    } ),
 StructField("transactn_type", StringType(), True,   
    {
   "heading" : "TTy", 
   "long_label" : "Transaction type", 
   "medium_label" : "Transactn type", 
   "conversion_routine" : "" 
    } ),
 StructField("transact_type", StringType(), True,   
    {
   "heading" : "TrTy", 
   "long_label" : "G/L Transaction Type", 
   "medium_label" : "Transact. Type", 
   "conversion_routine" : "" 
    } ),
 StructField("bustransaction", StringType(), True,   
    {
   "heading" : "BTran", 
   "long_label" : "Business Transaction", 
   "medium_label" : "Bus.Transaction", 
   "conversion_routine" : "" 
    } ),
 StructField("bus_trans_category", StringType(), True,   
    {
   "heading" : "BusTranCat", 
   "long_label" : "Business Transaction Category", 
   "medium_label" : "Bus. Trans. Category", 
   "conversion_routine" : "" 
    } ),
 StructField("bus_trans_type", StringType(), True,   
    {
   "heading" : "BusTranTyp", 
   "long_label" : "Business Transaction Type", 
   "medium_label" : "Bus. Trans. Type", 
   "conversion_routine" : "" 
    } ),
 StructField("closing_step", IntegerType(), True,   
    {
   "heading" : "Financial Closing Step", 
   "long_label" : "Closing Step", 
   "medium_label" : "Closing Step", 
   "conversion_routine" : "" 
    } ),
 StructField("ref_procedure", StringType(), True,   
    {
   "heading" : "Ref. proc.", 
   "long_label" : "Reference procedure", 
   "medium_label" : "Ref. procedure", 
   "conversion_routine" : "" 
    } ),
 StructField("logical_system", StringType(), True,   
    {
   "heading" : "Log.System", 
   "long_label" : "Log. system source", 
   "medium_label" : "Logical system", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("refer_orgunit", StringType(), True,   
    {
   "heading" : "Ref.org.un", 
   "long_label" : "Reference Org. Unit", 
   "medium_label" : "Refer. Org.Unit", 
   "conversion_routine" : "" 
    } ),
 StructField("reference_doc", StringType(), True,   
    {
   "heading" : "Ref. doc.", 
   "long_label" : "Reference document", 
   "medium_label" : "Reference doc.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("ref_doc_item", IntegerType(), True,   
    {
   "heading" : "Ref.It", 
   "long_label" : "Ref. Doc. Line Item", 
   "medium_label" : "Ref. Doc. Item", 
   "conversion_routine" : "" 
    } ),
 StructField("ref_item_group", IntegerType(), True,   
    {
   "heading" : "RefGrp", 
   "long_label" : "Ref. Doc. Item Group", 
   "medium_label" : "Ref. Item Group", 
   "conversion_routine" : "" 
    } ),
 StructField("sub_transaction", IntegerType(), True,   
    {
   "heading" : "SubTA", 
   "long_label" : "Sub Transaction", 
   "medium_label" : "Sub Transaction", 
   "conversion_routine" : "" 
    } ),
 StructField("is_reversing", StringType(), True,   
    {
   "heading" : "Rvrsng", 
   "long_label" : "Is Reversing Another Item", 
   "medium_label" : "Is Reversing", 
   "conversion_routine" : "" 
    } ),
 StructField("is_reversed", StringType(), True,   
    {
   "heading" : "Rvrsd", 
   "long_label" : "Is Reversed", 
   "medium_label" : "Is Reversed", 
   "conversion_routine" : "" 
    } ),
 StructField("is_true_reversal", StringType(), True,   
    {
   "heading" : "TruRev", 
   "long_label" : "Is true reversal", 
   "medium_label" : "Is true reversal", 
   "conversion_routine" : "" 
    } ),
 StructField("reversalreftran", StringType(), True,   
    {
   "heading" : "ReversTran", 
   "long_label" : "Reversal Ref. Trans.", 
   "medium_label" : "ReversalRefTran", 
   "conversion_routine" : "" 
    } ),
 StructField("reversal_org", StringType(), True,   
    {
   "heading" : "Rev. Org.", 
   "long_label" : "Reversal Organizatns", 
   "medium_label" : "Reversal Org.", 
   "conversion_routine" : "" 
    } ),
 StructField("reversal_ref", StringType(), True,   
    {
   "heading" : "Rev. Ref.", 
   "long_label" : "Reversal Ref. No.", 
   "medium_label" : "Reversal Ref.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("reversal_refdoc_li", IntegerType(), True,   
    {
   "heading" : "RvRfLI", 
   "long_label" : "Reversal: Reference Document Line Item", 
   "medium_label" : "Reversal Ref.Doc. LI", 
   "conversion_routine" : "" 
    } ),
 StructField("reversal_sub_trans", IntegerType(), True,   
    {
   "heading" : "RSubTA", 
   "long_label" : "Reversal Sub Transaction", 
   "medium_label" : "Reversal Sub Trans", 
   "conversion_routine" : "" 
    } ),
 StructField("is_settling", StringType(), True,   
    {
   "heading" : "Settlg", 
   "long_label" : "Is Settling/Transferring Another Item", 
   "medium_label" : "Is Settling", 
   "conversion_routine" : "" 
    } ),
 StructField("is_settled", StringType(), True,   
    {
   "heading" : "Settld", 
   "long_label" : "Is Settled/Transferred", 
   "medium_label" : "Is Settled", 
   "conversion_routine" : "" 
    } ),
 StructField("precreftransact", StringType(), True,   
    {
   "heading" : "PrecRefTrn", 
   "long_label" : "Preced. RefTransact", 
   "medium_label" : "PrecRefTransact", 
   "conversion_routine" : "" 
    } ),
 StructField("precreflogsys", StringType(), True,   
    {
   "heading" : "PrecLogSys", 
   "long_label" : "Preced. RefLogSystem", 
   "medium_label" : "PrecRefLogSys", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("precreforgunit", StringType(), True,   
    {
   "heading" : "PrecRefOrg", 
   "long_label" : "Preced. RefOrgUnit", 
   "medium_label" : "PrecRefOrgUnit", 
   "conversion_routine" : "" 
    } ),
 StructField("precrefdocument", StringType(), True,   
    {
   "heading" : "PrecRefDoc", 
   "long_label" : "Preced. RefDocument", 
   "medium_label" : "PrecRefDocument", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("prec_ref_doc_item", IntegerType(), True,   
    {
   "heading" : "PrReIt", 
   "long_label" : "Preceding Ref. Doc. Line Item", 
   "medium_label" : "Prec. Ref. Doc. Item", 
   "conversion_routine" : "" 
    } ),
 StructField("prec_sub_transactn", IntegerType(), True,   
    {
   "heading" : "PSubTA", 
   "long_label" : "Preceding Sub Transaction", 
   "medium_label" : "Prec. Sub Transactn", 
   "conversion_routine" : "" 
    } ),
 StructField("multprecrefid", StringType(), True,   
    {
   "heading" : "Multiple Preceding Doc. Ref. ID", 
   "long_label" : "Multiple PrecRef ID", 
   "medium_label" : "MultPrecRefID", 
   "conversion_routine" : "" 
    } ),
 StructField("precje_cocode", StringType(), True,   
    {
   "heading" : "PrecCoCode", 
   "long_label" : "Preceding JE CoCode", 
   "medium_label" : "PrecJE CoCode", 
   "conversion_routine" : "" 
    } ),
 StructField("precje_year", IntegerType(), True,   
    {
   "heading" : "PrecYear", 
   "long_label" : "Preceding JE Year", 
   "medium_label" : "PrecJE Year", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("precje_docno", StringType(), True,   
    {
   "heading" : "PrecDocNo", 
   "long_label" : "Preceding JE DocNo", 
   "medium_label" : "PrecJE DocNo", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("precje_lineitem", StringType(), True,   
    {
   "heading" : "PrecLnItm", 
   "long_label" : "PrecedingJE LineItem", 
   "medium_label" : "PrecJE LineItem", 
   "conversion_routine" : "" 
    } ),
 StructField("secondary_entry", StringType(), True,   
    {
   "heading" : "Sec", 
   "long_label" : "Secondary journal entry", 
   "medium_label" : "Secondary entry", 
   "conversion_routine" : "" 
    } ),
 StructField("closing_run_uuid", StringType(), True,   
    {
   "heading" : "UUID of Financial Closing Run", 
   "long_label" : "UUID of Financial Closing Run", 
   "medium_label" : "Closing Run UUID", 
   "conversion_routine" : "" 
    } ),
 StructField("orgl_change", StringType(), True,   
    {
   "heading" : "Organizational Change", 
   "long_label" : "Organizational Change", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("src_doc_type", StringType(), True,   
    {
   "heading" : "SrcDocType", 
   "long_label" : "Source Doc. Type", 
   "medium_label" : "Src Doc Type", 
   "conversion_routine" : "" 
    } ),
 StructField("src_doc_sys", StringType(), True,   
    {
   "heading" : "SrcDocSys", 
   "long_label" : "Source Doc. System", 
   "medium_label" : "Src Doc Sys", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("source_org_unit", StringType(), True,   
    {
   "heading" : "SrcDocOrg", 
   "long_label" : "Source Doc. Org Unit", 
   "medium_label" : "Source Org Unit", 
   "conversion_routine" : "" 
    } ),
 StructField("source_doc_no", StringType(), True,   
    {
   "heading" : "SrcDoc No.", 
   "long_label" : "Source Document No.", 
   "medium_label" : "Source Doc. No.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("source_doc_lineitem", IntegerType(), True,   
    {
   "heading" : "SrcItm", 
   "long_label" : "Source Document Line Item", 
   "medium_label" : "Source Doc. LineItem", 
   "conversion_routine" : "" 
    } ),
 StructField("source_doc_subitem", IntegerType(), True,   
    {
   "heading" : "ScDcSI", 
   "long_label" : "Source Document Subitem", 
   "medium_label" : "Source Doc. Subitem", 
   "conversion_routine" : "" 
    } ),
 StructField("commitment", StringType(), True,   
    {
   "heading" : "Commt", 
   "long_label" : "Commitment", 
   "medium_label" : "Commitment", 
   "conversion_routine" : "" 
    } ),
 StructField("obsoleteness_reason", StringType(), True,   
    {
   "heading" : "ObsRsn", 
   "long_label" : "Reason why this item is obsolete", 
   "medium_label" : "Obsoleteness reason", 
   "conversion_routine" : "" 
    } ),
 StructField("bal_transac_crcy", StringType(), True,   
    {
   "heading" : "BTCrcy", 
   "long_label" : "Balance Transaction Currency", 
   "medium_label" : "Bal. Transac. Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("transaction_currency", StringType(), True,   
    {
   "heading" : "TrCrcy", 
   "long_label" : "Transaction Currency", 
   "medium_label" : "Transaction Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("companycode_currency", StringType(), True,   
    {
   "heading" : "CCCrcy", 
   "long_label" : "Company Code Currency", 
   "medium_label" : "CompanyCode Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("global_currency", StringType(), True,   
    {
   "heading" : "Global Currency", 
   "long_label" : "Global Currency", 
   "medium_label" : "Global Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("functional_currency", StringType(), True,   
    {
   "heading" : "FuCrcy", 
   "long_label" : "Functional Currency", 
   "medium_label" : "Functional Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_1", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 1", 
   "long_label" : "Freely Defined Currency 1", 
   "medium_label" : "Free Defined Crcy 1", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_2", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 2", 
   "long_label" : "Freely Defined Currency 2", 
   "medium_label" : "Free Defined Crcy 2", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_3", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 3", 
   "long_label" : "Freely Defined Currency 3", 
   "medium_label" : "Free Defined Crcy 3", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_4", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 4", 
   "long_label" : "Freely Defined Currency 4", 
   "medium_label" : "Free Defined Crcy 4", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_5", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 5", 
   "long_label" : "Freely Defined Currency 5", 
   "medium_label" : "Free Defined Crcy 5", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_6", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 6", 
   "long_label" : "Freely Defined Currency 6", 
   "medium_label" : "Free Defined Crcy 6", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_7", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 7", 
   "long_label" : "Freely Defined Currency 7", 
   "medium_label" : "Free Defined Crcy 7", 
   "conversion_routine" : "" 
    } ),
 StructField("free_defined_crcy_8", StringType(), True,   
    {
   "heading" : "Freely Defined Currency 8", 
   "long_label" : "Freely Defined Currency 8", 
   "medium_label" : "Free Defined Crcy 8", 
   "conversion_routine" : "" 
    } ),
 StructField("co_object_currency", StringType(), True,   
    {
   "heading" : "ObCrcy", 
   "long_label" : "CO Object Currency", 
   "medium_label" : "CO Object Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("grant_currency", StringType(), True,   
    {
   "heading" : "GrantCurr", 
   "long_label" : "Grant Currency", 
   "medium_label" : "Grant Currency", 
   "conversion_routine" : "" 
    } ),
 StructField("base_unit", StringType(), True,   
    {
   "heading" : "BUn", 
   "long_label" : "Base Unit of Measure", 
   "medium_label" : "Base Unit", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("valuation_uom", StringType(), True,   
    {
   "heading" : "VUM", 
   "long_label" : "ValUnit of Measure", 
   "medium_label" : "Valuation UoM", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("reference_uom", StringType(), True,   
    {
   "heading" : "RUM", 
   "long_label" : "RefUnit of Measure", 
   "medium_label" : "Reference UoM", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("ref_quantity_type", StringType(), True,   
    {
   "heading" : "RQTy", 
   "long_label" : "Reference Quantity Type", 
   "medium_label" : "Ref. Quantity Type", 
   "conversion_routine" : "" 
    } ),
 StructField("inventory_uom", StringType(), True,   
    {
   "heading" : "IUM", 
   "long_label" : "Inventory Unit of Measure", 
   "medium_label" : "Inventory UoM", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("add_unit_meas_1", StringType(), True,   
    {
   "heading" : "UM1", 
   "long_label" : "Add Unit of Measure1", 
   "medium_label" : "Add Unit Meas 1", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("add_unit_meas_2", StringType(), True,   
    {
   "heading" : "UM2", 
   "long_label" : "Add Unit of Measure2", 
   "medium_label" : "Add Unit Meas 2", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("add_unit_meas_3", StringType(), True,   
    {
   "heading" : "UM3", 
   "long_label" : "Add Unit of Measure3", 
   "medium_label" : "Add Unit Meas 3", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("uom_covalqty", StringType(), True,   
    {
   "heading" : "UoMCOValQ", 
   "long_label" : "UoM CO Valuation Quantity", 
   "medium_label" : "UoM COValQty", 
   "conversion_routine" : "CUNIT" 
    } ),
 StructField("account_number", StringType(), True,   
    {
   "heading" : "Account", 
   "long_label" : "Account Number", 
   "medium_label" : "Account Number", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("cost_center", StringType(), True,   
    {
   "heading" : "Cost Ctr", 
   "long_label" : "Cost Center", 
   "medium_label" : "Cost Center", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("profit_center", StringType(), True,   
    {
   "heading" : "Profit Ctr", 
   "long_label" : "Profit Center", 
   "medium_label" : "Profit Center", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("functional_area", StringType(), True,   
    {
   "heading" : "Functional Area", 
   "long_label" : "Functional Area", 
   "medium_label" : "Functional Area", 
   "conversion_routine" : "" 
    } ),
 StructField("business_area", StringType(), True,   
    {
   "heading" : "BusA", 
   "long_label" : "Business Area", 
   "medium_label" : "Business Area", 
   "conversion_routine" : "" 
    } ),
 StructField("co_area", StringType(), True,   
    {
   "heading" : "COAr", 
   "long_label" : "Controlling Area", 
   "medium_label" : "CO Area", 
   "conversion_routine" : "" 
    } ),
 StructField("segment", StringType(), True,   
    {
   "heading" : "Segment", 
   "long_label" : "Segment", 
   "medium_label" : "Segment", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("sender_cost_ctr", StringType(), True,   
    {
   "heading" : "Sender CCtr", 
   "long_label" : "Sender cost center", 
   "medium_label" : "Sender cost ctr", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_pc", StringType(), True,   
    {
   "heading" : "Partner PC", 
   "long_label" : "Partner Profit Ctr", 
   "medium_label" : "Partner PC", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_farea", StringType(), True,   
    {
   "heading" : "PFAr", 
   "long_label" : "Partner Func. Area", 
   "medium_label" : "Partner FArea", 
   "conversion_routine" : "" 
    } ),
 StructField("trdg_partba", StringType(), True,   
    {
   "heading" : "TPBA", 
   "long_label" : "Trading Part.BA", 
   "medium_label" : "Trdg Part.BA", 
   "conversion_routine" : "" 
    } ),
 StructField("trading_partner", StringType(), True,   
    {
   "heading" : "Tr.Prt", 
   "long_label" : "Trading Partner No.", 
   "medium_label" : "Trading Partner", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_segment", StringType(), True,   
    {
   "heading" : "Ptnr Segm.", 
   "long_label" : "Partner Segment", 
   "medium_label" : "Partner Segment", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("amnt_in_bal_tr_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amnt in Balance Tr Crcy", 
   "long_label" : "Amount in Balance Transaction Currency", 
   "medium_label" : "Amnt in Bal Tr Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amnt_in_trans_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amnt in TransactionCrcy", 
   "long_label" : "Amount in Transaction Currency", 
   "medium_label" : "Amnt in Trans. Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("gv_amnt_in_transcrcy", DecimalType(23,2), True,   
    {
   "heading" : "GV Amnt in TransCrcy", 
   "long_label" : "Group Valuation Amnt in Transaction Crcy", 
   "medium_label" : "GV Amnt in TransCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("pcv_amnt_in_trnscrcy", DecimalType(23,2), True,   
    {
   "heading" : "PCV Amnt in TransCrcy", 
   "long_label" : "PrCtr Valuation Amnt in Transaction Crcy", 
   "medium_label" : "PCV Amnt in TrnsCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amnt_in_compcd_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amount in CompCd Crcy", 
   "long_label" : "Amount in Company Code Currency", 
   "medium_label" : "Amnt in CompCd Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amnt_in_gl_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Global Crcy", 
   "long_label" : "Amount in Global Currency", 
   "medium_label" : "Amnt in Gl. Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_functcrcy", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Functional Crcy", 
   "long_label" : "Amount in Functional Currency", 
   "medium_label" : "Amount in FunctCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_1", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 1", 
   "long_label" : "Amount in Freely Defined Currency 1", 
   "medium_label" : "Amount in Currency 1", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_2", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 2", 
   "long_label" : "Amount in Freely Defined Currency 2", 
   "medium_label" : "Amount in Currency 2", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_3", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 3", 
   "long_label" : "Amount in Freely Defined Currency 3", 
   "medium_label" : "Amount in Currency 3", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_4", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 4", 
   "long_label" : "Amount in Freely Defined Currency 4", 
   "medium_label" : "Amount in Currency 4", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_5", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 5", 
   "long_label" : "Amount in Freely Defined Currency 5", 
   "medium_label" : "Amount in Currency 5", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_6", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 6", 
   "long_label" : "Amount in Freely Defined Currency 6", 
   "medium_label" : "Amount in Currency 6", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_7", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 7", 
   "long_label" : "Amount in Freely Defined Currency 7", 
   "medium_label" : "Amount in Currency 7", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_currency_8", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Currency 8", 
   "long_label" : "Amount in Freely Defined Currency 8", 
   "medium_label" : "Amount in Currency 8", 
   "conversion_routine" : "" 
    } ),
 StructField("fixed_amnt_in_gc", DecimalType(23,2), True,   
    {
   "heading" : "Fixed Amnt in Glbl Crcy", 
   "long_label" : "Fixed Amount in Global Currency", 
   "medium_label" : "Fixed Amnt in GC", 
   "conversion_routine" : "" 
    } ),
 StructField("gv_fixd_amt_glb_crcy", DecimalType(23,2), True,   
    {
   "heading" : "GV Fix Amt Global Crcy", 
   "long_label" : "Group Val Fixed Amount in Global Crcy", 
   "medium_label" : "GV Fixd Amt Glb Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("pcv_fixdamt_glb_crcy", DecimalType(23,2), True,   
    {
   "heading" : "PCV Fix Amt Global Crcy", 
   "long_label" : "PrCtr Val Fixed Amount in Global Crcy", 
   "medium_label" : "PCV FixdAmt Glb Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("price_variance", DecimalType(23,2), True,   
    {
   "heading" : "Price Variance GlobCrcy", 
   "long_label" : "Total Price Variance in Global Crcy", 
   "medium_label" : "Price Variance", 
   "conversion_routine" : "" 
    } ),
 StructField("gval_tprice_varc_gc", DecimalType(23,2), True,   
    {
   "heading" : "Group Valuation Total Price Variance in Global Currency", 
   "long_label" : "Group Val Total Price Var in Global Crcy", 
   "medium_label" : "GVal TPrice Varc GC", 
   "conversion_routine" : "" 
    } ),
 StructField("pcval_tprice_varc_gc", DecimalType(23,2), True,   
    {
   "heading" : "PrCtr Valuation Total Price Variance in Global Currency", 
   "long_label" : "PrCtr Val Total Price Var in Global Crcy", 
   "medium_label" : "PCVal TPrice Varc GC", 
   "conversion_routine" : "" 
    } ),
 StructField("price_var_fxd", DecimalType(23,2), True,   
    {
   "heading" : "Price Var Fixed in GC", 
   "long_label" : "Fixed Price Variance in Global Crcy", 
   "medium_label" : "Price Var. Fxd", 
   "conversion_routine" : "" 
    } ),
 StructField("gval_fprice_varc_gc", DecimalType(23,2), True,   
    {
   "heading" : "Group Valuation Fixed Price Variance in Global Currency", 
   "long_label" : "Group ValFixed Price Var in Global Crcy", 
   "medium_label" : "GVal FPrice Varc GC", 
   "conversion_routine" : "" 
    } ),
 StructField("pcval_fprice_varc_gc", DecimalType(23,2), True,   
    {
   "heading" : "PrCtr Valuation Fixed Price Variance in Global Currency", 
   "long_label" : "PrCtr Val Fixed Price Var in Global Crcy", 
   "medium_label" : "PCVal FPrice Varc GC", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_obj_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amount Obj. Crcy", 
   "long_label" : "Amount in CO Object Currency", 
   "medium_label" : "Amount in Obj Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_in_grant_crcy", DecimalType(23,2), True,   
    {
   "heading" : "Amount in Grant Crcy", 
   "long_label" : "Amount in Grant Currency", 
   "medium_label" : "Amount in Grant Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_lc", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Local Crcy", 
   "long_label" : "AltValue Local Crcy", 
   "medium_label" : "AltValue LC", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_gc", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Group Crcy", 
   "long_label" : "AltValue in GrpCrcy", 
   "medium_label" : "AltValue GC", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_1", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 1", 
   "long_label" : "Alt.Value in Freely Defined Currency 1", 
   "medium_label" : "AltValue in Crcy 1", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_2", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 2", 
   "long_label" : "Alt.Value in Freely Defined Currency 2", 
   "medium_label" : "AltValue in Crcy 2", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_3", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 3", 
   "long_label" : "Alt.Value in Freely Defined Currency 3", 
   "medium_label" : "AltValue in Crcy 3", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_4", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 4", 
   "long_label" : "Alt.Value in Freely Defined Currency 4", 
   "medium_label" : "AltValue in Crcy 4", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_5", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 5", 
   "long_label" : "Alt.Value in Freely Defined Currency 5", 
   "medium_label" : "AltValue in Crcy 5", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_6", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 6", 
   "long_label" : "Alt.Value in Freely Defined Currency 6", 
   "medium_label" : "AltValue in Crcy 6", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_7", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 7", 
   "long_label" : "Alt.Value in Freely Defined Currency 7", 
   "medium_label" : "AltValue in Crcy 7", 
   "conversion_routine" : "" 
    } ),
 StructField("altvalue_in_crcy_8", DecimalType(23,2), True,   
    {
   "heading" : "AltValue in Currency 8", 
   "long_label" : "Alt.Value in Freely Defined Currency 8", 
   "medium_label" : "AltValue in Crcy 8", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_lc", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Local Crcy", 
   "long_label" : "ExtValue Local Crcy", 
   "medium_label" : "ExtValue LC", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_gc", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Group Crcy", 
   "long_label" : "ExtValue in GrpCrcy", 
   "medium_label" : "ExtValue GC", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_1", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 1", 
   "long_label" : "Ext.Value in Freely Defined Currency 1", 
   "medium_label" : "ExtValue in Crcy 1", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_2", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 2", 
   "long_label" : "Ext.Value in Freely Defined Currency 2", 
   "medium_label" : "ExtValue in Crcy 2", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_3", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 3", 
   "long_label" : "Ext.Value in Freely Defined Currency 3", 
   "medium_label" : "ExtValue in Crcy 3", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_4", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 4", 
   "long_label" : "Ext.Value in Freely Defined Currency 4", 
   "medium_label" : "ExtValue in Crcy 4", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_5", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 5", 
   "long_label" : "Ext.Value in Freely Defined Currency 5", 
   "medium_label" : "ExtValue in Crcy 5", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_6", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 6", 
   "long_label" : "Ext.Value in Freely Defined Currency 6", 
   "medium_label" : "ExtValue in Crcy 6", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_7", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 7", 
   "long_label" : "Ext.Value in Freely Defined Currency 7", 
   "medium_label" : "ExtValue in Crcy 7", 
   "conversion_routine" : "" 
    } ),
 StructField("extvalue_in_crcy_8", DecimalType(23,2), True,   
    {
   "heading" : "ExtValue in Currency 8", 
   "long_label" : "Ext.Value in Freely Defined Currency 8", 
   "medium_label" : "ExtValue in Crcy 8", 
   "conversion_routine" : "" 
    } ),
 StructField("value_sp_lcrcy", DecimalType(23,2), True,   
    {
   "heading" : "Value SP LocalCrcy", 
   "long_label" : "Value SP LocalCrcy", 
   "medium_label" : "Value SP LCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("quantity", DecimalType(23,1), True,   
    {
   "heading" : "Quantity", 
   "long_label" : "Quantity", 
   "medium_label" : "Quantity", 
   "conversion_routine" : "" 
    } ),
 StructField("fixed_quantity", DecimalType(23,1), True,   
    {
   "heading" : "Fixed quantity", 
   "long_label" : "Fixed quantity", 
   "medium_label" : "Fixed quantity", 
   "conversion_routine" : "" 
    } ),
 StructField("val_quantity", DecimalType(23,1), True,   
    {
   "heading" : "Valuation quantity", 
   "long_label" : "Valuation quantity", 
   "medium_label" : "Val. quantity", 
   "conversion_routine" : "" 
    } ),
 StructField("fixed_val_qty", DecimalType(23,1), True,   
    {
   "heading" : "Fixed valuation quant.", 
   "long_label" : "Fixed valuation qty", 
   "medium_label" : "Fixed val. qty", 
   "conversion_routine" : "" 
    } ),
 StructField("ref_quantity", DecimalType(23,1), True,   
    {
   "heading" : "Reference quantity", 
   "long_label" : "Reference quantity", 
   "medium_label" : "Ref. quantity", 
   "conversion_routine" : "" 
    } ),
 StructField("add_quantity_1", DecimalType(23,1), True,   
    {
   "heading" : "Add. Quantity 1", 
   "long_label" : "Add. Quantity 1", 
   "medium_label" : "Add. Quantity 1", 
   "conversion_routine" : "" 
    } ),
 StructField("add_quantity_2", DecimalType(23,1), True,   
    {
   "heading" : "Add. Quantity 2", 
   "long_label" : "Add. Quantity 2", 
   "medium_label" : "Add. Quantity 2", 
   "conversion_routine" : "" 
    } ),
 StructField("add_quantity_3", DecimalType(23,1), True,   
    {
   "heading" : "Add. Quantity 3", 
   "long_label" : "Add. Quantity 3", 
   "medium_label" : "Add. Quantity 3", 
   "conversion_routine" : "" 
    } ),
 StructField("covalqty", DecimalType(23,1), True,   
    {
   "heading" : "COValQty", 
   "long_label" : "CO Valuation Quantity", 
   "medium_label" : "COValQty", 
   "conversion_routine" : "" 
    } ),
 StructField("covalqtyfix", DecimalType(23,1), True,   
    {
   "heading" : "COValQtyF", 
   "long_label" : "CO Valuation Quantity Fix", 
   "medium_label" : "COValQtyFix", 
   "conversion_routine" : "" 
    } ),
 StructField("invvalue_lcrcy", DecimalType(23,2), True,   
    {
   "heading" : "InvValue LocalCrcy", 
   "long_label" : "InvValue LocalCrcy", 
   "medium_label" : "InvValue LCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("invvalue_gcrcy", DecimalType(23,2), True,   
    {
   "heading" : "InvValue GroupCrcy", 
   "long_label" : "InvValue GroupCrcy", 
   "medium_label" : "InvValue GCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("invvalue_fdc_1", DecimalType(23,2), True,   
    {
   "heading" : "InvValue Freely Defined Currency 1", 
   "long_label" : "InvValue Freely Defined Currency 1", 
   "medium_label" : "InvValue FDC 1", 
   "conversion_routine" : "" 
    } ),
 StructField("invvalue_fdc_2", DecimalType(23,2), True,   
    {
   "heading" : "InvValue Freely Defined Currency 2", 
   "long_label" : "InvValue Freely Defined Currency 2", 
   "medium_label" : "InvValue FDC 2", 
   "conversion_routine" : "" 
    } ),
 StructField("altinvvalue_lc", DecimalType(23,2), True,   
    {
   "heading" : "AltInvValue LocalCrcy", 
   "long_label" : "AltInvValue LC", 
   "medium_label" : "AltInvValue LC", 
   "conversion_routine" : "" 
    } ),
 StructField("altinvvalue_gc", DecimalType(23,2), True,   
    {
   "heading" : "AltInvValue GroupCrcy", 
   "long_label" : "AltInvValue GrpCrcy", 
   "medium_label" : "AltInvValue GC", 
   "conversion_routine" : "" 
    } ),
 StructField("altinvv_fdc_1", DecimalType(23,2), True,   
    {
   "heading" : "AltValue Freely Defined Currency 1", 
   "long_label" : "AltValue Freely Defined Currency 1", 
   "medium_label" : "AltInvV FDC 1", 
   "conversion_routine" : "" 
    } ),
 StructField("altinvv_fdc_2", DecimalType(23,2), True,   
    {
   "heading" : "AltValue Freely Defined Currency 2", 
   "long_label" : "AltValue Freely Defined Currency 2", 
   "medium_label" : "AltInvV FDC 2", 
   "conversion_routine" : "" 
    } ),
 StructField("map_lcrcy", DecimalType(23,2), True,   
    {
   "heading" : "MvAvgPrice LocalCrcy", 
   "long_label" : "MvAvgPrice LocalCrcy", 
   "medium_label" : "MAP LCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("map_gcrcy", DecimalType(23,2), True,   
    {
   "heading" : "MvAvgPrice GroupCrcy", 
   "long_label" : "MvAvgPrice GroupCrcy", 
   "medium_label" : "MAP GCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("map_acrcy", DecimalType(23,2), True,   
    {
   "heading" : "MvAvgPrice AnotherCrcy", 
   "long_label" : "MAP AnotherCrcy", 
   "medium_label" : "MAP ACrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("map_4crcy", DecimalType(23,2), True,   
    {
   "heading" : "MvAvgPrice FourthCrcy", 
   "long_label" : "MAP 4Crcy", 
   "medium_label" : "MAP 4Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("stdprice_lcrcy", DecimalType(23,2), True,   
    {
   "heading" : "StdPrice LocalCrcy", 
   "long_label" : "StdPrice LocalCrcy", 
   "medium_label" : "StdPrice LCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("stdprice_gcrcy", DecimalType(23,2), True,   
    {
   "heading" : "StdPrice GroupCrcy", 
   "long_label" : "StdPrice GroupCrcy", 
   "medium_label" : "StdPrice GCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("stdprice_acrcy", DecimalType(23,2), True,   
    {
   "heading" : "StdPrice AnotherCrcy", 
   "long_label" : "StdPrice AnotherCrcy", 
   "medium_label" : "StdPrice ACrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("stdprice_4crcy", DecimalType(23,2), True,   
    {
   "heading" : "StdPrice FourthCrcy", 
   "long_label" : "StdPrice 4Crcy", 
   "medium_label" : "StdPrice 4Crcy", 
   "conversion_routine" : "" 
    } ),
 StructField("invval_sp_lcrcy", DecimalType(23,2), True,   
    {
   "heading" : "InvVal SP LocalCrcy", 
   "long_label" : "InvVal SP LocalCrcy", 
   "medium_label" : "InvVal SP LCrcy", 
   "conversion_routine" : "" 
    } ),
 StructField("inv_quantity", DecimalType(23,1), True,   
    {
   "heading" : "Inventory Quantity", 
   "long_label" : "Inventory Quantity", 
   "medium_label" : "Inv. Quantity", 
   "conversion_routine" : "" 
    } ),
 StructField("debitcredit", StringType(), True,   
    {
   "heading" : "D/C", 
   "long_label" : "Debit/Credit ind", 
   "medium_label" : "Debit/Credit", 
   "conversion_routine" : "" 
    } ),
 StructField("posting_period", IntegerType(), True,   
    {
   "heading" : "Period", 
   "long_label" : "Posting period", 
   "medium_label" : "Posting period", 
   "conversion_routine" : "" 
    } ),
 StructField("fiyear_variant", StringType(), True,   
    {
   "heading" : "FV", 
   "long_label" : "Fiscal Year Variant", 
   "medium_label" : "Fi.Year Variant", 
   "conversion_routine" : "" 
    } ),
 StructField("periodyear", IntegerType(), True,   
    {
   "heading" : "Period", 
   "long_label" : "Period/Year", 
   "medium_label" : "Period/Year", 
   "conversion_routine" : "PERI7" 
    } ),
 StructField("posting_date", DateType(), True,   
    {
   "heading" : "Pstng Date", 
   "long_label" : "Posting Date", 
   "medium_label" : "Posting Date", 
   "conversion_routine" : "" 
    } ),
 StructField("document_date", DateType(), True,   
    {
   "heading" : "Doc. Date", 
   "long_label" : "Document Date", 
   "medium_label" : "Document Date", 
   "conversion_routine" : "" 
    } ),
 StructField("document_type", StringType(), True,   
    {
   "heading" : "Doc. Type", 
   "long_label" : "Document Type", 
   "medium_label" : "Document Type", 
   "conversion_routine" : "" 
    } ),
 StructField("item", IntegerType(), True,   
    {
   "heading" : "Itm", 
   "long_label" : "Item", 
   "medium_label" : "Item", 
   "conversion_routine" : "" 
    } ),
 StructField("assignment", StringType(), True,   
    {
   "heading" : "Assignment", 
   "long_label" : "Assignment", 
   "medium_label" : "Assignment", 
   "conversion_routine" : "" 
    } ),
 StructField("posting_key", StringType(), True,   
    {
   "heading" : "PK", 
   "long_label" : "Posting Key", 
   "medium_label" : "Posting Key", 
   "conversion_routine" : "" 
    } ),
 StructField("document_status", StringType(), True,   
    {
   "heading" : "S", 
   "long_label" : "Document Status", 
   "medium_label" : "Document Status", 
   "conversion_routine" : "" 
    } ),
 StructField("item_category", StringType(), True,   
    {
   "heading" : "Cat.", 
   "long_label" : "Item Category", 
   "medium_label" : "Item Category", 
   "conversion_routine" : "" 
    } ),
 StructField("transaction", StringType(), True,   
    {
   "heading" : "Trs", 
   "long_label" : "Transaction", 
   "medium_label" : "Transaction", 
   "conversion_routine" : "" 
    } ),
 StructField("slalineitemtype", IntegerType(), True,   
    {
   "heading" : "SLALiTy", 
   "long_label" : "SLA Line Item Type", 
   "medium_label" : "SLALineItemType", 
   "conversion_routine" : "" 
    } ),
 StructField("changed", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "Changed (Doc.split.)", 
   "medium_label" : "Changed", 
   "conversion_routine" : "" 
    } ),
 StructField("user_name", StringType(), True,   
    {
   "heading" : "User Name", 
   "long_label" : "User Name", 
   "medium_label" : "User Name", 
   "conversion_routine" : "" 
    } ),
 StructField("time_stamp", DecimalType(19, 0), True,   
    {
   "heading" : "Time Stamp", 
   "long_label" : "Time Stamp", 
   "medium_label" : "Time Stamp", 
   "conversion_routine" : "" 
    } ),
 StructField("elimination_prctr", StringType(), True,   
    {
   "heading" : "Elim.PrCtr", 
   "long_label" : "Elimination PrCtr", 
   "medium_label" : "Elimination PrCtr", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("origin_object", IntegerType(), True,   
    {
   "heading" : "Origin object", 
   "long_label" : "Origin object", 
   "medium_label" : "Origin object", 
   "conversion_routine" : "" 
    } ),
 StructField("gl_account_type", StringType(), True,   
    {
   "heading" : "G/L Account Type", 
   "long_label" : "G/L Account Type", 
   "medium_label" : "G/L Account Type", 
   "conversion_routine" : "" 
    } ),
 StructField("chart_of_accts", StringType(), True,   
    {
   "heading" : "Chart of Accounts", 
   "long_label" : "Chart of Accounts", 
   "medium_label" : "Chart of Accts", 
   "conversion_routine" : "" 
    } ),
 StructField("altern_account", StringType(), True,   
    {
   "heading" : "Alt. Acct", 
   "long_label" : "Alternative Account No.", 
   "medium_label" : "Altern. Account", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("alternative_coa", StringType(), True,   
    {
   "heading" : "ChA2", 
   "long_label" : "Alternative COA", 
   "medium_label" : "Alternative COA", 
   "conversion_routine" : "" 
    } ),
 StructField("consolidation_unit", StringType(), True,   
    {
   "heading" : "Cons. Unit", 
   "long_label" : "Consolidation Unit", 
   "medium_label" : "Consolidation Unit", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_unit", StringType(), True,   
    {
   "heading" : "Partner Unit", 
   "long_label" : "Partner Consolidation Unit", 
   "medium_label" : "Partner Unit", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("company", StringType(), True,   
    {
   "heading" : "Co.", 
   "long_label" : "Company", 
   "medium_label" : "Company", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("cons_coa", StringType(), True,   
    {
   "heading" : "CA", 
   "long_label" : "Consolidation COA", 
   "medium_label" : "Cons. COA", 
   "conversion_routine" : "" 
    } ),
 StructField("fs_item", StringType(), True,   
    {
   "heading" : "Financial Statement Item", 
   "long_label" : "Financial Statement Item", 
   "medium_label" : "FS Item", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("subitem_category", StringType(), True,   
    {
   "heading" : "SC", 
   "long_label" : "Subitem Category", 
   "medium_label" : "Subitem Category", 
   "conversion_routine" : "" 
    } ),
 StructField("subitem", StringType(), True,   
    {
   "heading" : "Subitem", 
   "long_label" : "Subitem", 
   "medium_label" : "Subitem", 
   "conversion_routine" : "" 
    } ),
 StructField("invoice_ref", StringType(), True,   
    {
   "heading" : "Inv. Ref.", 
   "long_label" : "Invoice Reference", 
   "medium_label" : "Invoice Ref.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("fiscal_year_rel_inv", IntegerType(), True,   
    {
   "heading" : "", 
   "long_label" : "Fiscal Year", 
   "medium_label" : "Fiscal Year Rel Inv", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("item_rebzz", IntegerType(), True,   
    {
   "heading" : "Itm", 
   "long_label" : "Item", 
   "medium_label" : "Item REBZZ", 
   "conversion_routine" : "" 
    } ),
 StructField("rebzt", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "Follow-On Doc. Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("po_category", IntegerType(), True,   
    {
   "heading" : "RPOC", 
   "long_label" : "PO Category", 
   "medium_label" : "PO Category", 
   "conversion_routine" : "REFBT" 
    } ),
 StructField("logsys_of_purchdoc", StringType(), True,   
    {
   "heading" : "LogSystem", 
   "long_label" : "Logical System of Purchasing Document", 
   "medium_label" : "LogSys of PurchDoc", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("purchasing_doc", StringType(), True,   
    {
   "heading" : "Pur. Doc.", 
   "long_label" : "Purchasing Document", 
   "medium_label" : "Purchasing Doc.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("item_ebelp", IntegerType(), True,   
    {
   "heading" : "Item", 
   "long_label" : "Item", 
   "medium_label" : "Item EBELP", 
   "conversion_routine" : "" 
    } ),
 StructField("account_assgmt_no", IntegerType(), True,   
    {
   "heading" : "SAA", 
   "long_label" : "Seq. No. of Account Assgt", 
   "medium_label" : "Account Assgmt No.", 
   "conversion_routine" : "" 
    } ),
 StructField("text", StringType(), True,   
    {
   "heading" : "Text", 
   "long_label" : "Text", 
   "medium_label" : "Text", 
   "conversion_routine" : "" 
    } ),
 StructField("sales_order", StringType(), True,   
    {
   "heading" : "Sales Ord.", 
   "long_label" : "Sales Order", 
   "medium_label" : "Sales Order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("sales_ord_item", IntegerType(), True,   
    {
   "heading" : "SO item", 
   "long_label" : "Sales order item", 
   "medium_label" : "Sales ord. item", 
   "conversion_routine" : "" 
    } ),
 StructField("material", StringType(), True,   
    {
   "heading" : "Material", 
   "long_label" : "Material", 
   "medium_label" : "Material", 
   "conversion_routine" : "MATN1" 
    } ),
 StructField("plant", StringType(), True,   
    {
   "heading" : "Plnt", 
   "long_label" : "Plant", 
   "medium_label" : "Plant", 
   "conversion_routine" : "" 
    } ),
 StructField("supplier", StringType(), True,   
    {
   "heading" : "Supplier", 
   "long_label" : "Supplier", 
   "medium_label" : "Supplier", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("customer", StringType(), True,   
    {
   "heading" : "Customer", 
   "long_label" : "Customer", 
   "medium_label" : "Customer", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("serv_rend_dte", DateType(), True,   
    {
   "heading" : "Serv.R.Dte", 
   "long_label" : "Serv. Rendered Date", 
   "medium_label" : "Serv. Rend. Dte", 
   "conversion_routine" : "" 
    } ),
 StructField("per_of_perf_start", DateType(), True,   
    {
   "heading" : "Start Date", 
   "long_label" : "Billing Period of Performance Start Date", 
   "medium_label" : "Per. of Perf. Start", 
   "conversion_routine" : "" 
    } ),
 StructField("per_of_perf_end", DateType(), True,   
    {
   "heading" : "End Date", 
   "long_label" : "Billing Period of Performance End Date", 
   "medium_label" : "Per. of Perf. End", 
   "conversion_routine" : "" 
    } ),
 StructField("condition_contract", StringType(), True,   
    {
   "heading" : "Condition Contract", 
   "long_label" : "Condition Contract", 
   "medium_label" : "Condition Contract", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("translation_dte", DateType(), True,   
    {
   "heading" : "TranslDate", 
   "long_label" : "Translation date", 
   "medium_label" : "Translation dte", 
   "conversion_routine" : "" 
    } ),
 StructField("profit_center_source", StringType(), True,   
    {
   "heading" : "PrftCtrSrc", 
   "long_label" : "Profit Center Drvtn Srce Type", 
   "medium_label" : "Profit Center Source", 
   "conversion_routine" : "" 
    } ),
 StructField("account_type", StringType(), True,   
    {
   "heading" : "AccTy", 
   "long_label" : "Account type", 
   "medium_label" : "Account type", 
   "conversion_routine" : "" 
    } ),
 StructField("special_gl_ind", StringType(), True,   
    {
   "heading" : "SG", 
   "long_label" : "Special G/L Ind.", 
   "medium_label" : "Special G/L Ind", 
   "conversion_routine" : "" 
    } ),
 StructField("tax_ctryreg", StringType(), True,   
    {
   "heading" : "Tax Country/Region", 
   "long_label" : "Tax Country/Region", 
   "medium_label" : "Tax Ctry/Reg.", 
   "conversion_routine" : "" 
    } ),
 StructField("tax_code", StringType(), True,   
    {
   "heading" : "Tx", 
   "long_label" : "Tax Code", 
   "medium_label" : "Tax Code", 
   "conversion_routine" : "" 
    } ),
 StructField("house_bank", StringType(), True,   
    {
   "heading" : "House Bk", 
   "long_label" : "House Bank", 
   "medium_label" : "House Bank", 
   "conversion_routine" : "" 
    } ),
 StructField("account_id", StringType(), True,   
    {
   "heading" : "Acct ID", 
   "long_label" : "Account ID", 
   "medium_label" : "Account ID", 
   "conversion_routine" : "" 
    } ),
 StructField("value_date", DateType(), True,   
    {
   "heading" : "Value Date", 
   "long_label" : "Value date", 
   "medium_label" : "Value date", 
   "conversion_routine" : "" 
    } ),
 StructField("oi_management", StringType(), True,   
    {
   "heading" : "OI Management", 
   "long_label" : "Open Item Management", 
   "medium_label" : "OI Management", 
   "conversion_routine" : "" 
    } ),
 StructField("clearing", DateType(), True,   
    {
   "heading" : "Clearing", 
   "long_label" : "Clearing Date", 
   "medium_label" : "Clearing", 
   "conversion_routine" : "" 
    } ),
 StructField("clrng_doc", StringType(), True,   
    {
   "heading" : "Clrng doc.", 
   "long_label" : "Clearing Document", 
   "medium_label" : "Clrng doc.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("clrg_fiscal_yr", IntegerType(), True,   
    {
   "heading" : "Year", 
   "long_label" : "Clearing Fiscal Year", 
   "medium_label" : "Clrg Fiscal Yr", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("deprec_area", IntegerType(), True,   
    {
   "heading" : "Ar.", 
   "long_label" : "Depreciation Area", 
   "medium_label" : "Deprec. Area", 
   "conversion_routine" : "" 
    } ),
 StructField("asset", StringType(), True,   
    {
   "heading" : "Asset", 
   "long_label" : "Asset", 
   "medium_label" : "Asset", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("sub-number", StringType(), True,   
    {
   "heading" : "SNo.", 
   "long_label" : "Sub-number", 
   "medium_label" : "Sub-number", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("asset_val_date", DateType(), True,   
    {
   "heading" : "AssetValDate", 
   "long_label" : "Asset Value Date", 
   "medium_label" : "Asset Val. Date", 
   "conversion_routine" : "" 
    } ),
 StructField("ast_transaction_type", StringType(), True,   
    {
   "heading" : "Asset Transaction Type", 
   "long_label" : "Asset Transaction Type", 
   "medium_label" : "Ast Transaction Type", 
   "conversion_routine" : "" 
    } ),
 StructField("transtype_cat", StringType(), True,   
    {
   "heading" : "Category", 
   "long_label" : "Trans. Type Category", 
   "medium_label" : "Trans.Type Cat.", 
   "conversion_routine" : "" 
    } ),
 StructField("deprec_period", IntegerType(), True,   
    {
   "heading" : "Per", 
   "long_label" : "Depreciation Period", 
   "medium_label" : "Deprec. Period", 
   "conversion_routine" : "" 
    } ),
 StructField("group_asset", StringType(), True,   
    {
   "heading" : "Group", 
   "long_label" : "Group Asset", 
   "medium_label" : "Group Asset", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("subnumber", StringType(), True,   
    {
   "heading" : "SNo.", 
   "long_label" : "Subnumber", 
   "medium_label" : "Subnumber", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("distr_rule_grp", IntegerType(), True,   
    {
   "heading" : "DRG", 
   "long_label" : "Distrib. Rule Group", 
   "medium_label" : "Distr. Rule Grp", 
   "conversion_routine" : "" 
    } ),
 StructField("asset_class", StringType(), True,   
    {
   "heading" : "Class", 
   "long_label" : "Asset Class", 
   "medium_label" : "Asset Class", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("account_determ", StringType(), True,   
    {
   "heading" : "Account Determination", 
   "long_label" : "Account Determination", 
   "medium_label" : "Account Determ.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_asset", StringType(), True,   
    {
   "heading" : "To Asset", 
   "long_label" : "Partner Asset", 
   "medium_label" : "Partner Asset", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_sub-no", StringType(), True,   
    {
   "heading" : "SNo.", 
   "long_label" : "Partner Subnumber", 
   "medium_label" : "Partner Sub-No.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("asset_subnumber", StringType(), True,   
    {
   "heading" : "SNo.", 
   "long_label" : "Asset Subnumber", 
   "medium_label" : "Asset Subnumber", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("trans_type", StringType(), True,   
    {
   "heading" : "Transaction Type", 
   "long_label" : "Transaction Type", 
   "medium_label" : "Trans. Type", 
   "conversion_routine" : "" 
    } ),
 StructField("asset_val_date_pn", DateType(), True,   
    {
   "heading" : "AssetValDate", 
   "long_label" : "Asset Value Date", 
   "medium_label" : "Asset Val. Date PN", 
   "conversion_routine" : "" 
    } ),
 StructField("orig_val_dat", DateType(), True,   
    {
   "heading" : "AsstVal Date", 
   "long_label" : "Orig. Val. Date", 
   "medium_label" : "Orig. Val. Dat", 
   "conversion_routine" : "" 
    } ),
 StructField("complretiremnt", StringType(), True,   
    {
   "heading" : "C", 
   "long_label" : "Complete Retirement", 
   "medium_label" : "Compl.Retiremnt", 
   "conversion_routine" : "" 
    } ),
 StructField("amount_posted", DecimalType(23,2), True,   
    {
   "heading" : "Amount Posted", 
   "long_label" : "Amount Posted", 
   "medium_label" : "Amount Posted", 
   "conversion_routine" : "AC132" 
    } ),
 StructField("percentage_rate", DecimalType(6, 2), True,   
    {
   "heading" : "% Rate", 
   "long_label" : "Percentage Rate", 
   "medium_label" : "Percentage Rate", 
   "conversion_routine" : "" 
    } ),
 StructField("man_proport_values", StringType(), True,   
    {
   "heading" : "Man. Proportional Values", 
   "long_label" : "Manual Proportional Values", 
   "medium_label" : "Man. Proport. Values", 
   "conversion_routine" : "" 
    } ),
 StructField("cost_estimateno", IntegerType(), True,   
    {
   "heading" : "Cost Est.No.", 
   "long_label" : "Cost Estimate Number", 
   "medium_label" : "Cost EstimateNo", 
   "conversion_routine" : "" 
    } ),
 StructField("price_control", StringType(), True,   
    {
   "heading" : "Pr.", 
   "long_label" : "Price control", 
   "medium_label" : "Price control", 
   "conversion_routine" : "" 
    } ),
 StructField("price_determ", StringType(), True,   
    {
   "heading" : "Price Determination", 
   "long_label" : "Material Price Determination: Control", 
   "medium_label" : "Price Determ.", 
   "conversion_routine" : "" 
    } ),
 StructField("valuation", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "Spec. stk valuation", 
   "medium_label" : "Valuation", 
   "conversion_routine" : "" 
    } ),
 StructField("vendor_stk_val", StringType(), True,   
    {
   "heading" : "VStock", 
   "long_label" : "Vendor stock valuation", 
   "medium_label" : "Vendor stk val.", 
   "conversion_routine" : "" 
    } ),
 StructField("special_stock", StringType(), True,   
    {
   "heading" : "S", 
   "long_label" : "Special Stock", 
   "medium_label" : "Special Stock", 
   "conversion_routine" : "" 
    } ),
 StructField("valu_timestamp", DecimalType(27, 7), True,   
    {
   "heading" : "Valuation TimeStamp", 
   "long_label" : "Valuation Time Stamp", 
   "medium_label" : "Valu. TimeStamp", 
   "conversion_routine" : "" 
    } ),
 StructField("sd_doc_of_inv", StringType(), True,   
    {
   "heading" : "SD Doc Inv", 
   "long_label" : "SD Doc of Inventory", 
   "medium_label" : "SD Doc of Inv.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("sd_item_of_inv", IntegerType(), True,   
    {
   "heading" : "SDItmInv", 
   "long_label" : "SD Item of Inventory", 
   "medium_label" : "SD Item of Inv.", 
   "conversion_routine" : "" 
    } ),
 StructField("wbselem_of_inv", IntegerType(), True,   
    {
   "heading" : "WBSElemInv", 
   "long_label" : "WBSElem of Inventory", 
   "medium_label" : "WBSElem of Inv.", 
   "conversion_routine" : "ABPSP" 
    } ),
 StructField("wbselem_of_inv_2", StringType(), True,   
    {
   "heading" : "WBSElement of Inventory", 
   "long_label" : "WBSElem of Inventory", 
   "medium_label" : "WBSElem of Inv. 2", 
   "conversion_routine" : "ABPSN" 
    } ),
 StructField("vendor_of_inv", StringType(), True,   
    {
   "heading" : "VendorInv", 
   "long_label" : "Vendor of Inventory", 
   "medium_label" : "Vendor of Inv.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("valuation_type", StringType(), True,   
    {
   "heading" : "Val. Type", 
   "long_label" : "Valuation Type", 
   "medium_label" : "Valuation Type", 
   "conversion_routine" : "" 
    } ),
 StructField("valuation_area", StringType(), True,   
    {
   "heading" : "ValA", 
   "long_label" : "Valuation area", 
   "medium_label" : "Valuation area", 
   "conversion_routine" : "" 
    } ),
 StructField("price_unit_lc", DecimalType(6, 0), True,   
    {
   "heading" : "per", 
   "long_label" : "Price Unit LocalCrcy", 
   "medium_label" : "Price Unit LC", 
   "conversion_routine" : "" 
    } ),
 StructField("price_unit_gc", DecimalType(6, 0), True,   
    {
   "heading" : "per", 
   "long_label" : "Price Unit GroupCrcy", 
   "medium_label" : "Price Unit GC", 
   "conversion_routine" : "" 
    } ),
 StructField("price_unit_ac", DecimalType(6, 0), True,   
    {
   "heading" : "per", 
   "long_label" : "Price Unit ACrcy", 
   "medium_label" : "Price Unit AC", 
   "conversion_routine" : "" 
    } ),
 StructField("price_unit_4c", DecimalType(6, 0), True,   
    {
   "heading" : "per", 
   "long_label" : "Price Unit 4Crcy", 
   "medium_label" : "Price Unit 4C", 
   "conversion_routine" : "" 
    } ),
 StructField("orig_proccat", StringType(), True,   
    {
   "heading" : "PCat", 
   "long_label" : "Orig. process catego", 
   "medium_label" : "Orig. proc.cat.", 
   "conversion_routine" : "" 
    } ),
 StructField("category", StringType(), True,   
    {
   "heading" : "Cat", 
   "long_label" : "Category", 
   "medium_label" : "Category", 
   "conversion_routine" : "" 
    } ),
 StructField("prcrmntaltproc", IntegerType(), True,   
    {
   "heading" : "Procurement Alternative/Process", 
   "long_label" : "Procure.alt./process", 
   "medium_label" : "PrcrmntAlt/proc", 
   "conversion_routine" : "" 
    } ),
 StructField("prod_proc_no", IntegerType(), True,   
    {
   "heading" : "Prod.Proc", 
   "long_label" : "Production Process No", 
   "medium_label" : "Prod. Proc. No.", 
   "conversion_routine" : "" 
    } ),
 StructField("period_type", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "Period Type", 
   "medium_label" : "Period Type", 
   "conversion_routine" : "" 
    } ),
 StructField("item_mat_ledger", IntegerType(), True,   
    {
   "heading" : "Item", 
   "long_label" : "Item", 
   "medium_label" : "Item mat ledger", 
   "conversion_routine" : "" 
    } ),
 StructField("inv_movement_categ", StringType(), True,   
    {
   "heading" : "Inventory Movement Category", 
   "long_label" : "Inventory Movement Category", 
   "medium_label" : "Inv Movement Categ", 
   "conversion_routine" : "" 
    } ),
 StructField("sender_cocode", StringType(), True,   
    {
   "heading" : "SenderCCde", 
   "long_label" : "Company Code in Sender System", 
   "medium_label" : "Sender CoCode", 
   "conversion_routine" : "" 
    } ),
 StructField("sender_gl_account", StringType(), True,   
    {
   "heading" : "Sender General Ledger Account", 
   "long_label" : "Sender General Ledger Account", 
   "medium_label" : "Sender GL Account", 
   "conversion_routine" : "" 
    } ),
 StructField("sender_acct_assgmt", StringType(), True,   
    {
   "heading" : "Sender Account Assignment", 
   "long_label" : "Sender Account Assignment", 
   "medium_label" : "Sender Acct Assgmt", 
   "conversion_routine" : "" 
    } ),
 StructField("sndr_acctassgmt_type", StringType(), True,   
    {
   "heading" : "Sender Account Assignment Type", 
   "long_label" : "Sender Account Assignment Type", 
   "medium_label" : "Sndr AcctAssgmt Type", 
   "conversion_routine" : "" 
    } ),
 StructField("object_number", StringType(), True,   
    {
   "heading" : "Object number", 
   "long_label" : "Object number", 
   "medium_label" : "Object number", 
   "conversion_routine" : "" 
    } ),
 StructField("co_subkey", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "CO subkey", 
   "medium_label" : "CO subkey", 
   "conversion_routine" : "" 
    } ),
 StructField("origin_group", StringType(), True,   
    {
   "heading" : "OrGp", 
   "long_label" : "Origin Group", 
   "medium_label" : "Origin Group", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_object", StringType(), True,   
    {
   "heading" : "Partner Object", 
   "long_label" : "Partner Object", 
   "medium_label" : "Partner Object", 
   "conversion_routine" : "" 
    } ),
 StructField("parobj_source", StringType(), True,   
    {
   "heading" : "ParObjSrc", 
   "long_label" : "Partner Obj. Source", 
   "medium_label" : "ParObj Source", 
   "conversion_routine" : "" 
    } ),
 StructField("source_object", StringType(), True,   
    {
   "heading" : "Source Object", 
   "long_label" : "Source Object", 
   "medium_label" : "Source Object", 
   "conversion_routine" : "" 
    } ),
 StructField("drcr_ind_co", StringType(), True,   
    {
   "heading" : "D/C CO", 
   "long_label" : "Dr/Cr indicator  CO", 
   "medium_label" : "Dr/Cr ind. CO", 
   "conversion_routine" : "BEKNZ" 
    } ),
 StructField("drcr_origin", StringType(), True,   
    {
   "heading" : "D/C", 
   "long_label" : "Dr/Cr Ind. (Origin)", 
   "medium_label" : "Dr/Cr (Origin)", 
   "conversion_routine" : "BEKNZ" 
    } ),
 StructField("debit_type", IntegerType(), True,   
    {
   "heading" : "Debit Type", 
   "long_label" : "Debit Type", 
   "medium_label" : "Debit Type", 
   "conversion_routine" : "" 
    } ),
 StructField("qty_is_incomplete", IntegerType(), True,   
    {
   "heading" : "Quantity Is Incomplete", 
   "long_label" : "Quantity Is Incomplete", 
   "medium_label" : "Qty Is Incomplete", 
   "conversion_routine" : "" 
    } ),
 StructField("offsetting_acct", StringType(), True,   
    {
   "heading" : "OffsetAcct", 
   "long_label" : "Offsetting Account", 
   "medium_label" : "Offsetting Acct", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("offstacct_type", StringType(), True,   
    {
   "heading" : "OffAct", 
   "long_label" : "Offsetting Account Type", 
   "medium_label" : "Offst.Acct Type", 
   "conversion_routine" : "" 
    } ),
 StructField("item_completed", StringType(), True,   
    {
   "heading" : "C", 
   "long_label" : "Item Completed", 
   "medium_label" : "Item Completed", 
   "conversion_routine" : "" 
    } ),
 StructField("personnel_no", IntegerType(), True,   
    {
   "heading" : "Pers.No.", 
   "long_label" : "Personnel Number", 
   "medium_label" : "Personnel No.", 
   "conversion_routine" : "" 
    } ),
 StructField("profit_segment", IntegerType(), True,   
    {
   "heading" : "Prof. Seg.", 
   "long_label" : "Profitab. Segmt No.", 
   "medium_label" : "Profit. segment", 
   "conversion_routine" : "" 
    } ),
 StructField("paobj_is_co_relevant", StringType(), True,   
    {
   "heading" : "PAObCO", 
   "long_label" : "PAObjNr relevant for CO compatibility", 
   "medium_label" : "PAObj is CO relevant", 
   "conversion_routine" : "" 
    } ),
 StructField("object_class", StringType(), True,   
    {
   "heading" : "ObjCl", 
   "long_label" : "Object Class", 
   "medium_label" : "Object Class", 
   "conversion_routine" : "SCOPE" 
    } ),
 StructField("logical_system_object", StringType(), True,   
    {
   "heading" : "LogSystem", 
   "long_label" : "Logical System", 
   "medium_label" : "Logical System Object", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_cocode", StringType(), True,   
    {
   "heading" : "PaCC", 
   "long_label" : "Partner CoCode", 
   "medium_label" : "Partner CoCode", 
   "conversion_routine" : "" 
    } ),
 StructField("partnerobjclass", StringType(), True,   
    {
   "heading" : "PObCl", 
   "long_label" : "Partner Object Class", 
   "medium_label" : "PartnerObjClass", 
   "conversion_routine" : "SCOPE" 
    } ),
 StructField("logical_system_partner", StringType(), True,   
    {
   "heading" : "LogSystem", 
   "long_label" : "Logical system", 
   "medium_label" : "Logical system Partner", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("valstrategy", StringType(), True,   
    {
   "heading" : "Str", 
   "long_label" : "Valuation strategy", 
   "medium_label" : "Val.strategy", 
   "conversion_routine" : "" 
    } ),
 StructField("origin_object_hk", StringType(), True,   
    {
   "heading" : "Origin obj", 
   "long_label" : "Origin Object", 
   "medium_label" : "Origin object HK", 
   "conversion_routine" : "" 
    } ),
 StructField("origin_order", StringType(), True,   
    {
   "heading" : "Orig Order", 
   "long_label" : "Origin Order Number", 
   "medium_label" : "Origin Order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("origcctr", StringType(), True,   
    {
   "heading" : "OrigCCtr", 
   "long_label" : "Origin CCtr", 
   "medium_label" : "OrigCCtr", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("origact", StringType(), True,   
    {
   "heading" : "OrgAct", 
   "long_label" : "Origin activity", 
   "medium_label" : "OrigAct.", 
   "conversion_routine" : "" 
    } ),
 StructField("sce:_bproc", StringType(), True,   
    {
   "heading" : "Sce: BProc.", 
   "long_label" : "Source: Bus. process", 
   "medium_label" : "Sce: BProc.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("origin_profit_center", StringType(), True,   
    {
   "heading" : "Origin PC", 
   "long_label" : "Origin Profit Center", 
   "medium_label" : "Origin Profit Center", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("origin_material", StringType(), True,   
    {
   "heading" : "Origin Material", 
   "long_label" : "Origin Material", 
   "medium_label" : "Origin Material", 
   "conversion_routine" : "MATN1" 
    } ),
 StructField("varc_origin_account", StringType(), True,   
    {
   "heading" : "Variance Origin Account", 
   "long_label" : "Variance Origin Account", 
   "medium_label" : "Varc. Origin Account", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("account_assignment", StringType(), True,   
    {
   "heading" : "Account Assignment", 
   "long_label" : "Account Assignment", 
   "medium_label" : "Account Assignment", 
   "conversion_routine" : "" 
    } ),
 StructField("object_type", StringType(), True,   
    {
   "heading" : "OTy", 
   "long_label" : "Object Type", 
   "medium_label" : "Object Type", 
   "conversion_routine" : "" 
    } ),
 StructField("activity_type", StringType(), True,   
    {
   "heading" : "ActTyp", 
   "long_label" : "Activity Type", 
   "medium_label" : "Activity Type", 
   "conversion_routine" : "" 
    } ),
 StructField("order", StringType(), True,   
    {
   "heading" : "Order", 
   "long_label" : "Order", 
   "medium_label" : "Order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("order_category", IntegerType(), True,   
    {
   "heading" : "Cat", 
   "long_label" : "Order category", 
   "medium_label" : "Order category", 
   "conversion_routine" : "" 
    } ),
 StructField("wbs_element", IntegerType(), True,   
    {
   "heading" : "WBS Element", 
   "long_label" : "WBS Element", 
   "medium_label" : "WBS Element", 
   "conversion_routine" : "ABPSP" 
    } ),
 StructField("wbs_element_ps", StringType(), True,   
    {
   "heading" : "WBS element", 
   "long_label" : "WBS element", 
   "medium_label" : "WBS element PS", 
   "conversion_routine" : "ABPSN" 
    } ),
 StructField("project_def", IntegerType(), True,   
    {
   "heading" : "Project definition", 
   "long_label" : "Project definition", 
   "medium_label" : "Project def.", 
   "conversion_routine" : "KONPD" 
    } ),
 StructField("project_def_2", StringType(), True,   
    {
   "heading" : "Project definition", 
   "long_label" : "Project definition", 
   "medium_label" : "Project def. 2", 
   "conversion_routine" : "ABPSN" 
    } ),
 StructField("network", StringType(), True,   
    {
   "heading" : "Network", 
   "long_label" : "Network", 
   "medium_label" : "Network", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("netwk_activity", StringType(), True,   
    {
   "heading" : "Netwk act.", 
   "long_label" : "Network activity", 
   "medium_label" : "Netwk activity", 
   "conversion_routine" : "NUMCV" 
    } ),
 StructField("business_process", StringType(), True,   
    {
   "heading" : "Bus. Process", 
   "long_label" : "Business Process", 
   "medium_label" : "Business Process", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("cost_object", StringType(), True,   
    {
   "heading" : "CostObject", 
   "long_label" : "Cost Object", 
   "medium_label" : "Cost Object", 
   "conversion_routine" : "" 
    } ),
 StructField("acctindicator", StringType(), True,   
    {
   "heading" : "AInd", 
   "long_label" : "Accounting Indicator", 
   "medium_label" : "AcctIndicator", 
   "conversion_routine" : "" 
    } ),
 StructField("resource", StringType(), True,   
    {
   "heading" : "Resource", 
   "long_label" : "Resource", 
   "medium_label" : "Resource", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("notification", StringType(), True,   
    {
   "heading" : "Notification", 
   "long_label" : "Notification", 
   "medium_label" : "Notification", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("service_doc_type", StringType(), True,   
    {
   "heading" : "Service Document Type", 
   "long_label" : "Service Document Type", 
   "medium_label" : "Service Doc. Type", 
   "conversion_routine" : "" 
    } ),
 StructField("service_document", StringType(), True,   
    {
   "heading" : "Service Document", 
   "long_label" : "Service Document", 
   "medium_label" : "Service Document", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("service_doc_item", IntegerType(), True,   
    {
   "heading" : "Service Document Item", 
   "long_label" : "Service Document Item", 
   "medium_label" : "Service Doc. Item", 
   "conversion_routine" : "" 
    } ),
 StructField("serv_contract_type", StringType(), True,   
    {
   "heading" : "SrvConType", 
   "long_label" : "Service Contract Type", 
   "medium_label" : "Serv. Contract Type", 
   "conversion_routine" : "" 
    } ),
 StructField("service_contract", StringType(), True,   
    {
   "heading" : "SrvContr", 
   "long_label" : "Service Contract", 
   "medium_label" : "Service Contract", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("srv_contract_item", IntegerType(), True,   
    {
   "heading" : "SCItem", 
   "long_label" : "Service Contract Item", 
   "medium_label" : "Srv Contract Item", 
   "conversion_routine" : "" 
    } ),
 StructField("solution_order", StringType(), True,   
    {
   "heading" : "SolnOrder", 
   "long_label" : "Solution Order", 
   "medium_label" : "Solution Order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("solution_order_item", IntegerType(), True,   
    {
   "heading" : "SOItem", 
   "long_label" : "Solution Order Item", 
   "medium_label" : "Solution Order Item", 
   "conversion_routine" : "" 
    } ),
 StructField("contract_provider", StringType(), True,   
    {
   "heading" : "Provider Contract", 
   "long_label" : "Provider Contract", 
   "medium_label" : "Contract Provider", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("contract_item", IntegerType(), True,   
    {
   "heading" : "Contract Item", 
   "long_label" : "Contract Item", 
   "medium_label" : "Contract Item", 
   "conversion_routine" : "" 
    } ),
 StructField("contract_accounting_rev", StringType(), True,   
    {
   "heading" : "Revenue Accounting Contract", 
   "long_label" : "Revenue Accounting Contract", 
   "medium_label" : "Contract Accounting Rev", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("pob", StringType(), True,   
    {
   "heading" : "Performance Obligation", 
   "long_label" : "Performance Obligation", 
   "medium_label" : "POB", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("operating_concern", StringType(), True,   
    {
   "heading" : "OpCo", 
   "long_label" : "Operating concern", 
   "medium_label" : "Operating concern", 
   "conversion_routine" : "" 
    } ),
 StructField("part_acct_assgmt", StringType(), True,   
    {
   "heading" : "Partner Account Assignment", 
   "long_label" : "Partner Account Assignment", 
   "medium_label" : "Part. Acct Assgmt", 
   "conversion_routine" : "" 
    } ),
 StructField("prtobject_type", StringType(), True,   
    {
   "heading" : "POT", 
   "long_label" : "Partner Object Type", 
   "medium_label" : "Prt.object type", 
   "conversion_routine" : "" 
    } ),
 StructField("paractvy", StringType(), True,   
    {
   "heading" : "ParAct", 
   "long_label" : "ParActivity", 
   "medium_label" : "ParActvy", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_order", StringType(), True,   
    {
   "heading" : "ParOrder", 
   "long_label" : "Partner order no.", 
   "medium_label" : "Partner order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("prtnr_ord_cat", IntegerType(), True,   
    {
   "heading" : "Prtnr Order Category", 
   "long_label" : "Partner Order Category", 
   "medium_label" : "Prtnr Ord. Cat.", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_wbs_element", IntegerType(), True,   
    {
   "heading" : "Partner WBS Element (internal)", 
   "long_label" : "Partner WBS Element (internal ID)", 
   "medium_label" : "Partner WBS Element", 
   "conversion_routine" : "ABPSP" 
    } ),
 StructField("partner_wbs_element_2", StringType(), True,   
    {
   "heading" : "Partner WBS Element", 
   "long_label" : "Partner Work Breakdown Structure Element", 
   "medium_label" : "Partner WBS Element 2", 
   "conversion_routine" : "ABPSN" 
    } ),
 StructField("partner_project_def", IntegerType(), True,   
    {
   "heading" : "Partner Project (internal ID)", 
   "long_label" : "Partner Project Definition (internal ID)", 
   "medium_label" : "Partner Project Def.", 
   "conversion_routine" : "KONPD" 
    } ),
 StructField("part_proj_def", StringType(), True,   
    {
   "heading" : "Partner Project Definition", 
   "long_label" : "Partner Project Def.", 
   "medium_label" : "Part. Proj. Def", 
   "conversion_routine" : "ABPSN" 
    } ),
 StructField("partner_salord", StringType(), True,   
    {
   "heading" : "P. SlsOrd", 
   "long_label" : "Partner Sales Order Number", 
   "medium_label" : "Partner SalOrd", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("part_slsord_item", IntegerType(), True,   
    {
   "heading" : "P. SOItem", 
   "long_label" : "Partner Sales Order Item", 
   "medium_label" : "Part. SlsOrd Item", 
   "conversion_routine" : "" 
    } ),
 StructField("partnerprfseg", IntegerType(), True,   
    {
   "heading" : "Partner PS", 
   "long_label" : "Partner prof.segment", 
   "medium_label" : "PartnerPrf.Seg.", 
   "conversion_routine" : "" 
    } ),
 StructField("part_proj_network", StringType(), True,   
    {
   "heading" : "Partner Project Network", 
   "long_label" : "Partner Project Network", 
   "medium_label" : "Part. Proj. Network", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partprojntwkacty", StringType(), True,   
    {
   "heading" : "Partner Project Network Activity", 
   "long_label" : "Part. Project Network Activity", 
   "medium_label" : "Part.Proj.Ntwk.Acty", 
   "conversion_routine" : "NUMCV" 
    } ),
 StructField("part_bus_process", StringType(), True,   
    {
   "heading" : "Partner Business Process", 
   "long_label" : "Partner Business Process", 
   "medium_label" : "Part. Bus. Process", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_cost_object", StringType(), True,   
    {
   "heading" : "Partner Cost Object", 
   "long_label" : "Partner Cost Object", 
   "medium_label" : "Partner Cost Object", 
   "conversion_routine" : "" 
    } ),
 StructField("par_service_doctype", StringType(), True,   
    {
   "heading" : "Partner Service Document Type", 
   "long_label" : "Partner Service Document Type", 
   "medium_label" : "Par. Service DocType", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_service_doc", StringType(), True,   
    {
   "heading" : "Partner Service Document", 
   "long_label" : "Partner Service Document", 
   "medium_label" : "Partner Service Doc.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("stat_acctasstype_1", StringType(), True,   
    {
   "heading" : "AATS1", 
   "long_label" : "Statistical account assignment type 1", 
   "medium_label" : "Stat. AcctAss.Type 1", 
   "conversion_routine" : "" 
    } ),
 StructField("stat_acctasstype_2", StringType(), True,   
    {
   "heading" : "AATS2", 
   "long_label" : "Statistical account assignment type 2", 
   "medium_label" : "Stat. AcctAss.Type 2", 
   "conversion_routine" : "" 
    } ),
 StructField("stat_acctasstype_3", StringType(), True,   
    {
   "heading" : "AATS3", 
   "long_label" : "Statistical account assignment type 3", 
   "medium_label" : "Stat. AcctAss.Type 3", 
   "conversion_routine" : "" 
    } ),
 StructField("item_co", IntegerType(), True,   
    {
   "heading" : "Itm", 
   "long_label" : "Document item", 
   "medium_label" : "Item CO ", 
   "conversion_routine" : "" 
    } ),
 StructField("document_number_co", StringType(), True,   
    {
   "heading" : "DocumentNo", 
   "long_label" : "Document Number", 
   "medium_label" : "Document Number CO", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("posting_row", IntegerType(), True,   
    {
   "heading" : "PRw", 
   "long_label" : "Posting Row", 
   "medium_label" : "Posting Row", 
   "conversion_routine" : "" 
    } ),
 StructField("co_postingrow_1", IntegerType(), True,   
    {
   "heading" : "PR1", 
   "long_label" : "CO Posting  Row 1", 
   "medium_label" : "CO PostingRow 1", 
   "conversion_routine" : "" 
    } ),
 StructField("co_postingrow_2", IntegerType(), True,   
    {
   "heading" : "PR2", 
   "long_label" : "CO Posting  Row 2", 
   "medium_label" : "CO PostingRow 2", 
   "conversion_routine" : "" 
    } ),
 StructField("co_postingrow_5", IntegerType(), True,   
    {
   "heading" : "PR5", 
   "long_label" : "CO Posting  Row 5", 
   "medium_label" : "CO PostingRow 5", 
   "conversion_routine" : "" 
    } ),
 StructField("co_postingrow_6", IntegerType(), True,   
    {
   "heading" : "PR6", 
   "long_label" : "CO Posting  Row 6", 
   "medium_label" : "CO PostingRow 6", 
   "conversion_routine" : "" 
    } ),
 StructField("co_postingrow_7", IntegerType(), True,   
    {
   "heading" : "PR7", 
   "long_label" : "CO Posting  Row 7", 
   "medium_label" : "CO PostingRow 7", 
   "conversion_routine" : "" 
    } ),
 StructField("posting_row_co", IntegerType(), True,   
    {
   "heading" : "PRw", 
   "long_label" : "Posting Row", 
   "medium_label" : "Posting Row CO", 
   "conversion_routine" : "" 
    } ),
 StructField("refpostrow_1", IntegerType(), True,   
    {
   "heading" : "RR1", 
   "long_label" : "RefDoc Posting row 1", 
   "medium_label" : "RefPostRow 1", 
   "conversion_routine" : "" 
    } ),
 StructField("refpostrow_2", IntegerType(), True,   
    {
   "heading" : "RR2", 
   "long_label" : "RefDoc Posting row 2", 
   "medium_label" : "RefPostRow 2", 
   "conversion_routine" : "" 
    } ),
 StructField("refpostrow_5", IntegerType(), True,   
    {
   "heading" : "RR5", 
   "long_label" : "RefDoc Posting row 5", 
   "medium_label" : "RefPostRow 5", 
   "conversion_routine" : "" 
    } ),
 StructField("refpostrow_6", IntegerType(), True,   
    {
   "heading" : "RR6", 
   "long_label" : "RefDoc Posting row 6", 
   "medium_label" : "RefPostRow 6", 
   "conversion_routine" : "" 
    } ),
 StructField("refpostrow_7", IntegerType(), True,   
    {
   "heading" : "RR7", 
   "long_label" : "RefDoc Posting row 7", 
   "medium_label" : "RefPostRow 7", 
   "conversion_routine" : "" 
    } ),
 StructField("overtime_category", StringType(), True,   
    {
   "heading" : "Overtime Category", 
   "long_label" : "Overtime Category", 
   "medium_label" : "Overtime Category", 
   "conversion_routine" : "" 
    } ),
 StructField("work_item_id", StringType(), True,   
    {
   "heading" : "Work Item ID", 
   "long_label" : "Work Item ID", 
   "medium_label" : "Work Item ID", 
   "conversion_routine" : "" 
    } ),
 StructField("object_id", IntegerType(), True,   
    {
   "heading" : "ID", 
   "long_label" : "Object ID", 
   "medium_label" : "Object ID", 
   "conversion_routine" : "" 
    } ),
 StructField("activity", StringType(), True,   
    {
   "heading" : "Act.", 
   "long_label" : "Activity", 
   "medium_label" : "Activity", 
   "conversion_routine" : "NUMCV" 
    } ),
 StructField("order_item_no", IntegerType(), True,   
    {
   "heading" : "Order item number", 
   "long_label" : "Order item number", 
   "medium_label" : "Order item no.", 
   "conversion_routine" : "" 
    } ),
 StructField("suboperation", StringType(), True,   
    {
   "heading" : "SOp", 
   "long_label" : "Suboperation", 
   "medium_label" : "Suboperation", 
   "conversion_routine" : "NUMCV" 
    } ),
 StructField("equipment", StringType(), True,   
    {
   "heading" : "Equipment", 
   "long_label" : "Equipment", 
   "medium_label" : "Equipment", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("functional_loc", StringType(), True,   
    {
   "heading" : "Functional Location", 
   "long_label" : "Functional Location", 
   "medium_label" : "Functional loc.", 
   "conversion_routine" : "TPLNR" 
    } ),
 StructField("assembly", StringType(), True,   
    {
   "heading" : "Assembly", 
   "long_label" : "Assembly", 
   "medium_label" : "Assembly", 
   "conversion_routine" : "MATN1" 
    } ),
 StructField("maintactivtype", StringType(), True,   
    {
   "heading" : "MAT", 
   "long_label" : "MaintActivityType", 
   "medium_label" : "MaintActivType", 
   "conversion_routine" : "" 
    } ),
 StructField("orderplanind", StringType(), True,   
    {
   "heading" : "OPI", 
   "long_label" : "Order planning ind.", 
   "medium_label" : "OrderPlanInd.", 
   "conversion_routine" : "" 
    } ),
 StructField("prioritytype", StringType(), True,   
    {
   "heading" : "PrTyp", 
   "long_label" : "Priority Type", 
   "medium_label" : "PriorityType", 
   "conversion_routine" : "" 
    } ),
 StructField("priority", StringType(), True,   
    {
   "heading" : "P", 
   "long_label" : "Priority", 
   "medium_label" : "Priority", 
   "conversion_routine" : "" 
    } ),
 StructField("superior_order", StringType(), True,   
    {
   "heading" : "Superior Order", 
   "long_label" : "Superior Order", 
   "medium_label" : "Superior Order", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("material_group", StringType(), True,   
    {
   "heading" : "Matl Group", 
   "long_label" : "Material Group", 
   "medium_label" : "Material Group", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_order_item", IntegerType(), True,   
    {
   "heading" : "Partner Order Item Number", 
   "long_label" : "Partner Order Item Number", 
   "medium_label" : "Partner Order item", 
   "conversion_routine" : "" 
    } ),
 StructField("planned_partswork", StringType(), True,   
    {
   "heading" : "Plnd", 
   "long_label" : "Planned Parts/Work", 
   "medium_label" : "Planned Parts/Work", 
   "conversion_routine" : "" 
    } ),
 StructField("billing_type", StringType(), True,   
    {
   "heading" : "BillT", 
   "long_label" : "Billing Type", 
   "medium_label" : "Billing Type", 
   "conversion_routine" : "" 
    } ),
 StructField("sales_org", StringType(), True,   
    {
   "heading" : "SOrg.", 
   "long_label" : "Sales Organization", 
   "medium_label" : "Sales Org.", 
   "conversion_routine" : "" 
    } ),
 StructField("distr_channel", StringType(), True,   
    {
   "heading" : "DChl", 
   "long_label" : "Distribution Channel", 
   "medium_label" : "Distr. Channel", 
   "conversion_routine" : "" 
    } ),
 StructField("division", StringType(), True,   
    {
   "heading" : "Dv", 
   "long_label" : "Division", 
   "medium_label" : "Division", 
   "conversion_routine" : "" 
    } ),
 StructField("product_sold", StringType(), True,   
    {
   "heading" : "Product Sold", 
   "long_label" : "Product Sold", 
   "medium_label" : "Product Sold", 
   "conversion_routine" : "MATN1" 
    } ),
 StructField("product_sold_group", StringType(), True,   
    {
   "heading" : "ProdSoldGp", 
   "long_label" : "Product Sold Group", 
   "medium_label" : "Product Sold Group", 
   "conversion_routine" : "" 
    } ),
 StructField("customer_group", StringType(), True,   
    {
   "heading" : "CGrp", 
   "long_label" : "Customer Group", 
   "medium_label" : "Customer Group", 
   "conversion_routine" : "" 
    } ),
 StructField("ctryreg", StringType(), True,   
    {
   "heading" : "C/R", 
   "long_label" : "Country/Region Key", 
   "medium_label" : "Ctry/Reg.", 
   "conversion_routine" : "" 
    } ),
 StructField("industry", StringType(), True,   
    {
   "heading" : "Indus.", 
   "long_label" : "Industry", 
   "medium_label" : "Industry", 
   "conversion_routine" : "" 
    } ),
 StructField("sales_district", StringType(), True,   
    {
   "heading" : "SDst", 
   "long_label" : "Sales District", 
   "medium_label" : "Sales District", 
   "conversion_routine" : "" 
    } ),
 StructField("bill-to_party", StringType(), True,   
    {
   "heading" : "Bill-to", 
   "long_label" : "Bill-to Party", 
   "medium_label" : "Bill-to Party", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("ship-to_party", StringType(), True,   
    {
   "heading" : "Ship-to", 
   "long_label" : "Ship-to Party", 
   "medium_label" : "Ship-to Party", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("group", StringType(), True,   
    {
   "heading" : "Group", 
   "long_label" : "Group key", 
   "medium_label" : "Group", 
   "conversion_routine" : "" 
    } ),
 StructField("acdoc_copa_eew_dummy_pa", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("so_header_division", StringType(), True,   
    {
   "heading" : "SO Header Division", 
   "long_label" : "SO Header Division", 
   "medium_label" : "SO Header Division", 
   "conversion_routine" : "" 
    } ),
 StructField("cash_origin_cocode", StringType(), True,   
    {
   "heading" : "Cash Origin Company Code", 
   "long_label" : "Cash Origin Company Code", 
   "medium_label" : "Cash Origin CoCode", 
   "conversion_routine" : "" 
    } ),
 StructField("cash_origin_account", StringType(), True,   
    {
   "heading" : "Cash Origin Account", 
   "long_label" : "Cash Origin Account", 
   "medium_label" : "Cash Origin Account", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("fm_area", StringType(), True,   
    {
   "heading" : "FMA", 
   "long_label" : "FM Area", 
   "medium_label" : "FM Area", 
   "conversion_routine" : "" 
    } ),
 StructField("commitment_item", StringType(), True,   
    {
   "heading" : "Commitment Item", 
   "long_label" : "Commitment Item", 
   "medium_label" : "Commitment Item", 
   "conversion_routine" : "FMCIL" 
    } ),
 StructField("funds_center", StringType(), True,   
    {
   "heading" : "Funds Ctr", 
   "long_label" : "Funds Center", 
   "medium_label" : "Funds Center", 
   "conversion_routine" : "" 
    } ),
 StructField("funded_program", StringType(), True,   
    {
   "heading" : "Funded Program", 
   "long_label" : "Funded Program", 
   "medium_label" : "Funded Program", 
   "conversion_routine" : "" 
    } ),
 StructField("fund", StringType(), True,   
    {
   "heading" : "Fund", 
   "long_label" : "Fund", 
   "medium_label" : "Fund", 
   "conversion_routine" : "" 
    } ),
 StructField("grant", StringType(), True,   
    {
   "heading" : "Grant", 
   "long_label" : "Grant", 
   "medium_label" : "Grant", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("budget_period", StringType(), True,   
    {
   "heading" : "Budget Period", 
   "long_label" : "Budget Period", 
   "medium_label" : "Budget Period", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_fund", StringType(), True,   
    {
   "heading" : "Partner Fund", 
   "long_label" : "Partner Fund", 
   "medium_label" : "Partner Fund", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_grant", StringType(), True,   
    {
   "heading" : "Partner Grant", 
   "long_label" : "Partner Grant", 
   "medium_label" : "Partner Grant", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("par_budper", StringType(), True,   
    {
   "heading" : "P.Bud.Per.", 
   "long_label" : "Par. Budget Per.", 
   "medium_label" : "Par. BudPer", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_account", StringType(), True,   
    {
   "heading" : "Budget Account", 
   "long_label" : "Budget Account", 
   "medium_label" : "", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("bdgt_account_cocode", StringType(), True,   
    {
   "heading" : "Company Code for Budget Account", 
   "long_label" : "Company Code for Budget Account", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_cnsmpn_date", DateType(), True,   
    {
   "heading" : "Budget Consumption Date", 
   "long_label" : "Budget Consumption Date", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_cnsmpn_period", IntegerType(), True,   
    {
   "heading" : "Posting Period for Budget Consumption", 
   "long_label" : "Posting Period for Budget Consumption", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_cnsmpn_year", IntegerType(), True,   
    {
   "heading" : "Year of Budget Consumption", 
   "long_label" : "Year of Budget Consumption", 
   "medium_label" : "", 
   "conversion_routine" : "GJAHR" 
    } ),
 StructField("bdgt_relevant", StringType(), True,   
    {
   "heading" : "Budget-Relevant", 
   "long_label" : "Budget-Relevant", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_cnsmpn_type", StringType(), True,   
    {
   "heading" : "Budget Consumption Type", 
   "long_label" : "Budget Consumption Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_cnsmpn_amount_type", StringType(), True,   
    {
   "heading" : "Amount Type for Budget Consumption", 
   "long_label" : "Amount Type for Budget Consumption", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("sponsored_program", StringType(), True,   
    {
   "heading" : "Sponsored Program", 
   "long_label" : "Sponsored Program", 
   "medium_label" : "Sponsored Program", 
   "conversion_routine" : "" 
    } ),
 StructField("sponsored_class", StringType(), True,   
    {
   "heading" : "Sponsored Class", 
   "long_label" : "Sponsored Class", 
   "medium_label" : "Sponsored Class", 
   "conversion_routine" : "" 
    } ),
 StructField("bdgt_validty_no", StringType(), True,   
    {
   "heading" : "Budget Validity Number", 
   "long_label" : "Budget Validity Number", 
   "medium_label" : "Bdgt Validty No.", 
   "conversion_routine" : "" 
    } ),
 StructField("earmarked_funds", StringType(), True,   
    {
   "heading" : "", 
   "long_label" : "Earmarked Funds", 
   "medium_label" : "Earmarked funds", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("document_item", IntegerType(), True,   
    {
   "heading" : "Itm", 
   "long_label" : "Document Item", 
   "medium_label" : "Document Item", 
   "conversion_routine" : "" 
    } ),
 StructField("joint_venture", StringType(), True,   
    {
   "heading" : "JV", 
   "long_label" : "Joint venture", 
   "medium_label" : "Joint venture", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("equity_group", StringType(), True,   
    {
   "heading" : "EGr", 
   "long_label" : "Equity group", 
   "medium_label" : "Equity group", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("recovery_ind", StringType(), True,   
    {
   "heading" : "RI", 
   "long_label" : "Recovery Indicator", 
   "medium_label" : "Recovery Ind.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner", StringType(), True,   
    {
   "heading" : "Partner", 
   "long_label" : "Partner", 
   "medium_label" : "Partner", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("billing_ind", StringType(), True,   
    {
   "heading" : "BI", 
   "long_label" : "Billing indicator", 
   "medium_label" : "Billing ind.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("equity_type", StringType(), True,   
    {
   "heading" : "ET", 
   "long_label" : "Equity type", 
   "medium_label" : "Equity type", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("prodmonth", DateType(), True,   
    {
   "heading" : "ProdM.", 
   "long_label" : "Production Month", 
   "medium_label" : "Prod.Month", 
   "conversion_routine" : "" 
    } ),
 StructField("billing_month", DateType(), True,   
    {
   "heading" : "Bill.Month", 
   "long_label" : "Billing Month", 
   "medium_label" : "Billing Month", 
   "conversion_routine" : "" 
    } ),
 StructField("procopermonth", DateType(), True,   
    {
   "heading" : "POM", 
   "long_label" : "Proc. Operational Month", 
   "medium_label" : "Proc.Oper.Month", 
   "conversion_routine" : "" 
    } ),
 StructField("cutback_run_id_jva", DecimalType(27, 7), True,   
    {
   "heading" : "Cutback Run ID (Joint Venture Accounting)", 
   "long_label" : "Cutback Run ID (JVA)", 
   "medium_label" : "Cutback Run ID (JVA)", 
   "conversion_routine" : "" 
    } ),
 StructField("partner_venture_jva", StringType(), True,   
    {
   "heading" : "Partner Venture (Joint Venture Accounting)", 
   "long_label" : "Partner Venture (JVA)", 
   "medium_label" : "Partner Venture JVA", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("partner_eg_jva", StringType(), True,   
    {
   "heading" : "Partner Equity Group (Joint Venture Accounting)", 
   "long_label" : "Partner Equity Group (JVA)", 
   "medium_label" : "Partner EG (JVA)", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("sender_rec_ind", StringType(), True,   
    {
   "heading" : "S_RI", 
   "long_label" : "Sender Rec. Ind.", 
   "medium_label" : "Sender Rec. Ind", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("cutback_account_jva", StringType(), True,   
    {
   "heading" : "Cutback Account (Joint Venture Accounting)", 
   "long_label" : "Cutback Account (JVA)", 
   "medium_label" : "Cutback Account JVA", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("cutback_co_jva", StringType(), True,   
    {
   "heading" : "Cutback Cost Object (Joint Venture Accounting)", 
   "long_label" : "Cutback Cost Object (JVA)", 
   "medium_label" : "Cutback CO (JVA)", 
   "conversion_routine" : "" 
    } ),
 StructField("business_entity", StringType(), True,   
    {
   "heading" : "BE", 
   "long_label" : "Business Entity", 
   "medium_label" : "Business Entity", 
   "conversion_routine" : "SWENR" 
    } ),
 StructField("building", StringType(), True,   
    {
   "heading" : "Building", 
   "long_label" : "Building", 
   "medium_label" : "Building", 
   "conversion_routine" : "SGENR" 
    } ),
 StructField("land", StringType(), True,   
    {
   "heading" : "Land", 
   "long_label" : "Land", 
   "medium_label" : "Land", 
   "conversion_routine" : "SGRNR" 
    } ),
 StructField("rental_object", StringType(), True,   
    {
   "heading" : "RntlObj", 
   "long_label" : "Rental Object", 
   "medium_label" : "Rental Object", 
   "conversion_routine" : "SMENR" 
    } ),
 StructField("contract", StringType(), True,   
    {
   "heading" : "Contract", 
   "long_label" : "Contract", 
   "medium_label" : "Contract", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("servcharge_key", StringType(), True,   
    {
   "heading" : "SCK", 
   "long_label" : "Service Charge Key", 
   "medium_label" : "Serv.charge key", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("settlement_unit", StringType(), True,   
    {
   "heading" : "SU", 
   "long_label" : "Settlement Unit", 
   "medium_label" : "Settlement Unit", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("reference_date", DateType(), True,   
    {
   "heading" : "Ref date", 
   "long_label" : "Reference date", 
   "medium_label" : "Reference date", 
   "conversion_routine" : "" 
    } ),
 StructField("ptnr_bus_entity", StringType(), True,   
    {
   "heading" : "Ptnr. BE", 
   "long_label" : "Partner Business Entity", 
   "medium_label" : "Ptnr. Bus.  Entity", 
   "conversion_routine" : "SWENR" 
    } ),
 StructField("ptnr_building", StringType(), True,   
    {
   "heading" : "P. Build.", 
   "long_label" : "Partner Building", 
   "medium_label" : "Ptnr. Building", 
   "conversion_routine" : "SGENR" 
    } ),
 StructField("partner_land", StringType(), True,   
    {
   "heading" : "P. Land", 
   "long_label" : "Partner Land", 
   "medium_label" : "Partner Land", 
   "conversion_routine" : "SGRNR" 
    } ),
 StructField("ptnr_rent_unit", StringType(), True,   
    {
   "heading" : "P. RU", 
   "long_label" : "Partner Rent. Unit", 
   "medium_label" : "Ptnr. Rent. Unit", 
   "conversion_routine" : "SMENR" 
    } ),
 StructField("ptnr_contract_no", StringType(), True,   
    {
   "heading" : "P. Contr. No.", 
   "long_label" : "Partner Contract Number", 
   "medium_label" : "Ptnr. Contract No.", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("ptnr_srv_chrg_key", StringType(), True,   
    {
   "heading" : "P. SCK", 
   "long_label" : "Partner Servic. Charge Key", 
   "medium_label" : "Ptnr. Srv. Chrg. Key", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("ptnr_sett_unit", StringType(), True,   
    {
   "heading" : "P. SU", 
   "long_label" : "Partner Settlement Unit", 
   "medium_label" : "Ptnr. Sett. Unit", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("ptnr_reference_date", DateType(), True,   
    {
   "heading" : "P. RefDate", 
   "long_label" : "Partner Reference Date", 
   "medium_label" : "Ptnr. Reference Date", 
   "conversion_routine" : "" 
    } ),
 StructField("accrual_object_type", StringType(), True,   
    {
   "heading" : "Accrual Object Type", 
   "long_label" : "Accrual Object Type", 
   "medium_label" : "Accrual Object Type", 
   "conversion_routine" : "" 
    } ),
 StructField("logical_syst_acrobj", StringType(), True,   
    {
   "heading" : "ACR Log. System", 
   "long_label" : "Logical System of Accrual Object", 
   "medium_label" : "Logical Syst. AcrObj", 
   "conversion_routine" : "ALPHA" 
    } ),
 StructField("accrual_object", StringType(), True,   
    {
   "heading" : "Accrual Object", 
   "long_label" : "Accrual Object", 
   "medium_label" : "Accrual Object", 
   "conversion_routine" : "" 
    } ),
 StructField("accrual_subobject", StringType(), True,   
    {
   "heading" : "Accrual Subobject", 
   "long_label" : "Accrual Subobject", 
   "medium_label" : "Accrual Subobject", 
   "conversion_routine" : "" 
    } ),
 StructField("accrual_item_type", StringType(), True,   
    {
   "heading" : "Accrual Item Type", 
   "long_label" : "Accrual Item Type", 
   "medium_label" : "Accrual Item Type", 
   "conversion_routine" : "" 
    } ),
 StructField("acr_reference_id", StringType(), True,   
    {
   "heading" : "Accrual Reference ID", 
   "long_label" : "Accrual Reference ID", 
   "medium_label" : "Acr.  Reference ID", 
   "conversion_routine" : "" 
    } ),
 StructField("accrual_value_date", DateType(), True,   
    {
   "heading" : "AccrValDte", 
   "long_label" : "Accrual Value Date", 
   "medium_label" : "Accrual Value Date", 
   "conversion_routine" : "" 
    } ),
 StructField("type_of_finvalobj", StringType(), True,   
    {
   "heading" : "Type of the Financial Valualtion Object", 
   "long_label" : "Type of the Financial Valuation Object", 
   "medium_label" : "Type of Fin.Val.Obj.", 
   "conversion_routine" : "" 
    } ),
 StructField("fin_valuation_object", StringType(), True,   
    {
   "heading" : "Financial Valuation Object", 
   "long_label" : "Financial Valuation Object", 
   "medium_label" : "Fin Valuation Object", 
   "conversion_routine" : "" 
    } ),
 StructField("finvalsubobject", StringType(), True,   
    {
   "heading" : "Financial Valuation SubObject", 
   "long_label" : "Financial Valuation SubObject", 
   "medium_label" : "Fin.Val.Sub.Object", 
   "conversion_routine" : "" 
    } ),
 StructField("due_on", DateType(), True,   
    {
   "heading" : "", 
   "long_label" : "Net Due Date", 
   "medium_label" : "Due On", 
   "conversion_routine" : "" 
    } ),
 StructField("risk_class", StringType(), True,   
    {
   "heading" : "Risk Class", 
   "long_label" : "Risk Class", 
   "medium_label" : "Risk Class", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_supnum_cob", StringType(), True,   
    {
   "heading" : "Supplier", 
   "long_label" : "Supplier", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_ordtyp_cob", StringType(), True,   
    {
   "heading" : "Order Type", 
   "long_label" : "Order Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_s2ctry_cob", StringType(), True,   
    {
   "heading" : "Ship To Country Code", 
   "long_label" : "Ship To Country Code", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_s2cust_cob", StringType(), True,   
    {
   "heading" : "Ship To Customer Number", 
   "long_label" : "Ship To Customer Number", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_subtyp_cob", StringType(), True,   
    {
   "heading" : "Subledger Type", 
   "long_label" : "Subledger Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_trnorg_cob", StringType(), True,   
    {
   "heading" : "Transaction Origin", 
   "long_label" : "Transaction Origin", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_ledtyp_cob", StringType(), True,   
    {
   "heading" : "Ledger Type", 
   "long_label" : "Ledger Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_ponumb_cob", StringType(), True,   
    {
   "heading" : "PO Number", 
   "long_label" : "PO Number", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_recflg_cob", StringType(), True,   
    {
   "heading" : "Reconciled", 
   "long_label" : "Reconciled", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_sernmb_cob", StringType(), True,   
    {
   "heading" : "Serial Number", 
   "long_label" : "Serial Number", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_subled_cob", StringType(), True,   
    {
   "heading" : "Subledger", 
   "long_label" : "Subledger", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_srcnum_cob", StringType(), True,   
    {
   "heading" : "Source Document Number", 
   "long_label" : "Source Document Number", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_doccmp_cob", StringType(), True,   
    {
   "heading" : "Document Company", 
   "long_label" : "Document Company", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_doctyp_cob", StringType(), True,   
    {
   "heading" : "Document Type", 
   "long_label" : "Document Type", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("zz1_docnum_cob", StringType(), True,   
    {
   "heading" : "Org Doc Number", 
   "long_label" : "Org Doc Number", 
   "medium_label" : "", 
   "conversion_routine" : "" 
    } ),
 StructField("follow-up_action", StringType(), True,   
    {
   "heading" : "FuA", 
   "long_label" : "Follow-up action", 
   "medium_label" : "Follow-up action", 
   "conversion_routine" : "" 
    } ),
 StructField("sdm_versioning", StringType(), True,   
    {
   "heading" : "SV", 
   "long_label" : "SDM Versioning", 
   "medium_label" : "SDM Versioning", 
   "conversion_routine" : "" 
    } ),
 StructField("migr_source", StringType(), True,   
    {
   "heading" : "MS", 
   "long_label" : "Migration Source", 
   "medium_label" : "Migr. Source", 
   "conversion_routine" : "" 
    } ),
 StructField("migr_line_item_id", StringType(), True,   
    {
   "heading" : "MigLIt", 
   "long_label" : "Migrated Line Item ID", 
   "medium_label" : "Migr. Line Item ID", 
   "conversion_routine" : "" 
    } ),
 StructField("data_aging", DateType(), True,   
    {
   "heading" : "Data Aging Filter", 
   "long_label" : "Data Aging Filter", 
   "medium_label" : "Data Aging", 
   "conversion_routine" : "" 
    } ),
 ]
 





# COMMAND ----------

schema_target = StructType(  fields_target + fields_technical )


# COMMAND ----------


from datetime import timedelta, datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
#from pyspark.sql.functions import sha2, concat_ws
spark.sql("use catalog development1")
spark.sql("use schema gl1_s")

batch_start_time = datetime.utcnow()

if not spark.catalog.tableExists("acdoca"):
    print(f"Target table does not exist, creating")
    empty_df = spark.createDataFrame ([], schema_target)
    empty_df.write.mode("overwrite").saveAsTable("acdoca")

max_upd_time = spark.sql("select max(dl_update_tm) from acdoca").first()[0]
print(max_upd_time)
#spark.sql("select * from gl1_b.acdoca where ld_update_tm > ")



