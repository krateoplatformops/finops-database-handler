def get_focus_create() -> str:
    return '''
AvailabilityZone STRING,
BilledCost DOUBLE,
BillingAccountId STRING,
BillingAccountName STRING,
BillingCurrency STRING,
BillingPeriodEnd TIMESTAMP,
BillingPeriodStart TIMESTAMP,
CapacityReservationId STRING,
CapacityReservationStatus STRING,
ChargeCategory STRING,
ChargeClass STRING,
ChargeDescription STRING,
ChargeFrequency STRING,
ChargePeriodEnd TIMESTAMP,
ChargePeriodStart TIMESTAMP,
CommitmentDiscountCategory STRING,
CommitmentDiscountId STRING,
CommitmentDiscountName STRING,
CommitmentDiscountQuantity DOUBLE,
CommitmentDiscountStatus STRING,
CommitmentDiscountType STRING,
CommitmentDiscountUnit STRING,
ConsumedUnit STRING,
ConsumedQuantity STRING,
ContractedCost DOUBLE,
ContractedUnitCost DOUBLE,
EffectiveCost DOUBLE,
InvoiceIssuerName STRING,
ListCost DOUBLE,
ListUnitPrice DOUBLE,
PricingCategory STRING,
PricingQuantity DOUBLE,
PricingUnit STRING,
ProviderName STRING,
PublisherName STRING,
RegionId STRING,
RegionName STRING,
ResourceId STRING,
ResourceName STRING,
ResourceType STRING,
ServiceCategory STRING,
ServiceName STRING,
ServiceSubcategory STRING,
SkuId STRING,
SkuMeter STRING,
SkuPriceDetails STRING,
SkuPriceId STRING,
SubAccountId STRING,
SubAccountName STRING,
Tags STRING,
PRIMARY KEY (ResourceId, ResourceName, BillingPeriodEnd, BillingPeriodStart, ChargePeriodEnd, ChargePeriodStart)
'''


def get_notebook_create() -> str:
    return '''
NOTEBOOK_NAME string,
DATA text,
PRIMARY KEY (NOTEBOOK_NAME)
'''

def get_resource_create() -> str:
    return '''
ResourceId string,
metricName string,
timestamp TIMESTAMP,
average double,
Unit string,
PRIMARY KEY (ResourceId,MetricName,timestamp)
'''

def get_generic_create() -> str:
    return '''
VALUE double
'''
