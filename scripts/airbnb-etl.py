# import necessary dependencies
import logging
import re
import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (col, udf, lit, concat_ws, DataFrame, row_number,
                                    regexp_replace, when, split, explode, trim, desc, 
                                    from_unixtime, unix_timestamp, monotonically_increasing_id)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from awsglue.transforms import ApplyMapping
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


logging.basicConfig(
    level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Initialise Spark Context and Glue Context and job
    logger.info("Initialising SparkContext...")
    sc = SparkContext()
    logger.info("SparkContext initialised successfully.")
    
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    glueContext.spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # conversion of time to 24hours
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    logger.info("Spark session initialised successfully.")
    
except Exception as e:
    logger.error("Error initialising Spark session or Glue job: %s", e)
    raise
finally:
    if 'job' in locals():
        job.commit()




def create_dynamic_frame_from_csv(path):
    """
    Creates a dynamic frame from a CSV path
  
    Parameter path (str): The CSV path to create a dynamic frame
    
    Returns: The dynamic frame created from the CSV path
    """
    try:
        logger.info("Creating dynamic frame from path: %s", path)
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            format_options={"withHeader": True},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [path], "recurse": True},
            transformation_ctx="airbnbETLpipeline"
        )
        logger.info("Dynamic frame created successfully from path: %s", path)
        return dynamic_frame
    except Exception as e:
        logger.error("Error creating dynamic frame from path: %s", path)
        logger.error(e)
        raise


def map_data_columns():
    """
    Defines the column mappings for the data transformation.

    Returns:
        list: A list of tuples containing the source column name, source column type,
              target column name, and target column type.
    """
    mapped_data = [
            ("Listing Title", "string", "listingTitle", "string"),
            ("Property Type", "string", "propertyType", "string"),
            ("Listing Type", "string", "listingType", "string"),
            ("Created Date", "string", "createdDate", "date"),
            ("Country", "string", "country", "string"),
            ("State", "string", "state", "string"),
            ("City", "string", "city", "string"),
            ("Zipcode", "string", "zipcode", "string"),
            ("Currency Native", "string", "currencyNative", "string"),
            ("Number of Reviews", "string", "numberOfReviews", "int"),
            ("Bedrooms", "string", "bedrooms", "int"), 
            ("Bathrooms", "string", "bathrooms", "int"), 
            ("Max Guests", "string", "maxGuests", "int"),
            ("Airbnb Superhost", "string", "superhost", "string" ),
            ("Cancellation Policy", "string", "cancellationPolicy", "string"),
            ("Cleaning Fee (USD)", "string", "cleaningFeeUSD", "float"),
            ("Cleaning Fee (Native)", "string", "cleaningFeeNative", "float"),
            ("Extra People Fee(Native)", "string", "extraPeopleFeeNative", "float"),
            ("Extra People Fee (USD)", "string", "extraPeopleFeeUSD", "float"),
            ("Check-in Time", "string", "checkinTime", "string"),
            ("Checkout Time", "string", "checkoutTime", "string"),
            ("Minimum Stay", "string", "minimumStay", "int"),
            ("Latitude", "string", "latitude", "float"),
            ("Longitude", "string", "longitude", "float"),  
            ("Exact Location", "string", "exactLocation", "string"),
            ("Overall Rating", "string", "overallRating", "float"),
            ("Airbnb Accuracy Rating", "string", "accuracyRating", "int"),
            ("Airbnb Communication Rating", "string", "communicationRating", "int"),
            ("Airbnb Value Rating", "string", "valueRating", "int"),
            ("Airbnb Checkin Rating", "string", "checkinRating", "int"),
            ("Airbnb Cleanliness Rating", "string", "cleanlinessRating", "int"),
            ("Airbnb Location Rating", "string", "locationRating", "int"), 
            ("Amenities", "string", "amenities", "string"),
            ("picture_url", "string", "pictureURL", "string"),            
            ("License", "string", "license", "string"),
            ("Airbnb Property ID", "string", "airbnbPropertyId", "string"),            
            ("Airbnb Host ID", "string", "airbnbHostId", "string"),
            ("Host Listing Count", "string", "hostListingCount", "int"),
            ("guest_controls", "string", "guestControls", "string"),
            ("instant_bookable", "string", "instantBookable", "string"),
            ("Pets Allowed", "string", "petsAllowed", "string"),
            ("Listing URL", "string", "listingURL", "string"),
            ("Instantbook Enabled", "string", "instantBookEnabled", "string"),
            ("Count Available Days LTM", "string", "countavailableDaysLtm", "int"),
            ("Count Blocked Days LTM", "string", "countBlockedDaysLtm", "int"), 
            ("Count Reservation Days LTM", "string", "countReservationDaysLtm", "int"),
            ("Occupancy Rate LTM", "string", "occupancyRateLtm", "int"),
            ("Number of Bookings LTM", "string", "numberOfBookingsLtm", "int"),
            ("Number of Bookings LTM - Number of observed month", "string", "numberOfBookingsLtmObservedMonth", "int"),
            ("Average Daily Rate (Native)", "string", "averageDailyRateNative", "float"), 
            ("Average Daily Rate (USD)", "string", "averageDailyRateUSD", "float"),
            ("Annual Revenue LTM (USD)", "string", "annualRevenueLtmUSD", "float"), 
            ("Annual Revenue LTM (Native)", "string", "annualRevenueLtmNative", "float"), 
        ]
    
    return mapped_data




def apply_mapping(dynamic_frame: DynamicFrame) -> DynamicFrame:
    """
    Maps columns in a dynamic frame
  
    Args:
    dynamic_frame: The dynamic frame to map columns
    dynamic_frame: The dynamic frame to map columns
    
    Returns: The mapped dynamic frame
    """
    mapped_data = map_data_columns()
    try:
        logger.info("Mapping columns in dynamic frame...")
        mapped_dynamic_frame = ApplyMapping.apply(
            frame=dynamic_frame,
            mappings=mapped_data,
            transformation_ctx="apply_mapping_ctx"
        )

        logger.info("Columns mapped successfully.")
        return mapped_dynamic_frame
    except Exception as e:
        logger.error("Error mapping columns: %s", e)
        raise


def drop_duplicates_and_common_missing_values(df: DataFrame) -> DataFrame:
    """
    Drops duplicates and rows with common missing values in the dataframe.

    Args:
    df: The dataframe to drop common missing values from

    Returns: The dataframe with common missing values dropped
    """
    try:
        logger.info("Dropping rows with common missing values.")

        missing_condition = (
            col("propertyType").isNull() & 
            col("listingType").isNull() & 
            col("bedrooms").isNull()
        )
        cleaned_df = df.filter(~missing_condition)
        df = cleaned_df.dropDuplicates()
        logger.info("Rows with common missing values dropped successfully.")
        return df
    except Exception as e:
        logger.error("Returnung df due to error dropping common missing values: %s", e)
        return df


def remove_emojis_from_text(text):
    
    """
    Removes emojis from text/row

    Args:
    text: The text to remove emojis from

    Returns: The text with emojis removed
    """
    try:
        logger.info("Removing emojis from text.")
        emoji_patterns = re.compile("(?u)["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
            u"\U00002700-\U000027BF"  # Dingbats
            u"\U00002600-\U000026FF"  # Miscellaneous Symbols
            u"\U00002B00-\U00002BFF"  # Miscellaneous Symbols and Arrows
            u"\U0001F100-\U0001F1FF"  # Enclosed Alphanumeric Supplement
            u"\U0001F200-\U0001F2FF"  # Enclosed Ideographic Supplement
            u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            u" \U000025A0-\U000025FF" # Geometric Shapes
              +"]")
        
        if isinstance(text, str):
            cleaned_text = emoji_patterns.sub(" ", text)
            logger.info("Emojis removed successfully.")
            return cleaned_text
        else:
            return text
    except Exception as e:
        logger.error("Error removing emojis from text: %s", e)
        raise


# create a user defined function to remove emojis
remove_emojis_udf = udf(remove_emojis_from_text)


def remove_emojis_from_df(df: DataFrame) -> DataFrame:
    """
    Removes emojis from all the columns in a dataframe

    Args:
    df: The dataframe to remove emojis from

    Returns: The dataframe with emojis removed
    """
    try:
        logger.info("Removing emojis from dataframe.")
        for col_name in df.columns:
            df = df.withColumn(col_name, remove_emojis_udf(col(col_name)))
        logger.info("Emojis removed from dataframe successfully.")
        return df
    except Exception as e:
        logger.error("Returnung df due to error removing emojis from dataframe: %s", e)
        return df



def standardised_columns(df: DataFrame,
                                country: str,
                                city: str = None,
                                state: str = None,
                                currencyNative: str = None
                            ) -> DataFrame:
    """
    Standardises the columns in the dataframe

    Args:
    df: The dataframe to standardise columns
    country: The country to add to the dataframe
    city: The city to add to the dataframe
    state: The state to add to the dataframe
    currency_native: The currency native to add to the dataframe

    Returns: The dataframe with standardised columns
    """
    try:
        logger.info("Standardising columns in dataframe.")
        df = df.withColumn("country", lit(country))
        df = df.withColumn("city", lit(city))
        df = df.withColumn("state", lit(state))
        df = df.withColumn("currencyNative", lit(currencyNative))
        logger.info("Columns standardised successfully.")
        return df
    except Exception as e:
        logger.error("Returnung df due to error standardising columns: {e}")
        logger.debug(e)
        return df



def create_id_columns(df_name: str, df: DataFrame) -> DataFrame:
    """
    Creates new ID columns for dimension tables.

    Args:
    df_name: The name of the dataframe
    df: The dataframe to create ID columns for

    Returns: The dataframe with new ID columns
    """
    # List of IDs to be generated
    id_columns = {
        "listingId": "LIS",
        "amenitiesId": "AMT",
        "reviewId": "REV",
        "checkInOutId": "CHK",
        "locationId": "LOC",
        "pricingId": "PRC",
        "bookingId": "BOK"
    }

    try:
        logger.info("Creating ID columns for dataframe: %s", df_name)
        
        # Create a prefix based on the first three characters of the dataframe name
        prefix = df_name[:3].upper()

        # Add a monotonically increasing ID column for ordering
        df = df.withColumn("monotonically_increasing_id", monotonically_increasing_id() + 1)

        # Generate the IDs
        for col_name, suffix in id_columns.items():
            df = df.withColumn(
                col_name,
                concat_ws(
                    "",
                    lit(prefix),
                    lit(suffix),
                     monotonically_increasing_id() + 1)
                )

        # Drop the temporary column used for ordering
        df = df.drop("monotonically_increasing_id")

        logger.info("ID columns created successfully for dataframe: %s", df_name)
        return df
    except Exception as e:
        logger.error("Returnung df due to error creating ID columns for dataframe: %s", df_name)
        logger.error(e)
        raise


def check_and_fix_columns(df: DataFrame) -> DataFrame:
    """
    Checks the DataFrame's columns against the required set of columns and fixes missing columns.

    Args:
        df (DataFrame): The Spark DataFrame to check and fix.

    Returns:
        DataFrame: The fixed DataFrame with all required columns present.
    """
    required_columns = {
        "listingId", "airbnbPropertyId", "listingTitle", "propertyType", "superhost", 
        "listingType", "listingURL", "createdDate", "currencyNative",
        "airbnbHostId", "hostListingCount", "locationId", "country", "state", 
        "city", "zipcode", "longitude", "latitude", "exactLocation",
        "amenitiesId", "amenities", "reviewId", "pricingId", "locationRating", "communicationRating",
        "overallRating",  "accuracyRating", "valueRating", "checkinRating", "pictureURL",
        "cleanlinessRating", "checkInOutId", "checkinTime", "checkoutTime", "minimumStay", 
        "occupancyRateLtm", "maxGuests", "numberOfReviews", "bedrooms", "bathrooms",
        "cancellationPolicy", "cleaningFeeUSD", "cleaningFeeNative",
        "extraPeopleFeeUSD", "extraPeopleFeeNative", "instantBookable", "petsAllowed", 
        "numberOfBookingsLtm", "numberOfBookingsLtmObservedMonth", "averageDailyRateUSD",
        "averageDailyRateNative", "countBlockedDaysLtm", "instantBookEnabled",
        "countReservationDaysLtm", "countavailableDaysLtm", "annualRevenueLtmUSD", "annualRevenueLtmNative"
    }

    try:
        current_columns = set(df.columns)
        
        # Find missing columns
        missing_columns = required_columns - current_columns
        if missing_columns:
            logger.warning("DataFrame is missing columns: %s", missing_columns)
            
            # Add missing columns with null values
            for column in missing_columns:
                logger.info("Adding missing column to the DataFrame: %s", column)
                df = df.withColumn(column, lit(None))

        logger.info("DataFrame columns checked and fixed successfully.")
        return df
    except Exception as e:
        logger.error("Returnung df due to error in check_and_fix_columns: %s", e)
        return df



def define_dimension_columns() -> str:
    """
    Define the columns for the dimension tables.
    """

    listing_columns = ["listingId", "airbnbPropertyId", "bedrooms", "bathrooms", "maxGuests", "listingTitle", "propertyType", 
                        "listingType", "listingURL", "pictureURL", "createdDate",   
                        "cancellationPolicy", "instantBookable", 
                        "petsAllowed"]
    
    host_columns = ["airbnbHostId", "hostListingCount" ,"superhost"]

    location_columns = ["locationId", "country", "state", "city", "zipcode", 
                        "longitude", "latitude", "exactLocation" ]
    
    amenities_columns = ["amenitiesId", "amenities"]

    review_columns = ["reviewId", "numberOfReviews", "locationRating", "communicationRating", 
                        "overallRating", "accuracyRating", "valueRating", "checkinRating",
                         "cleanlinessRating"]

    checkin_checkout_columns = ["checkInOutId", "checkinTime", "checkoutTime", "minimumStay"]
    
    pricing_columns = ["pricingId", "currencyNative", "cleaningFeeUSD", "cleaningFeeNative", "extraPeopleFeeUSD",
                        "extraPeopleFeeNative", "averageDailyRateUSD", "averageDailyRateNative"]
    
    booking_columns = ["bookingId", "airbnbHostId", "listingId", "amenitiesId",
                        "airbnbPropertyId", "reviewId", "checkInOutId", "locationId",
                        "pricingId",  "occupancyRateLtm", "numberOfBookingsLtm",
                        "numberOfBookingsLtmObservedMonth",  "countBlockedDaysLtm", 
                        "instantBookEnabled", "countReservationDaysLtm", 
                        "countavailableDaysLtm", "annualRevenueLtmUSD", "annualRevenueLtmNative"]

    return listing_columns, host_columns, location_columns, amenities_columns, review_columns, checkin_checkout_columns, pricing_columns, booking_columns



def fill_listing_dim(listing_dim: DataFrame) -> DataFrame:
    """
    Fill missing values in the listing_dim DataFrame with default 
    values and remove double quotes from the listingTitle column.

    Args:
        listing_dim (DataFrame): The input DataFrame containing listing data.

    Returns:
        DataFrame: The DataFrame with missing values filled and listingTitle column cleaned.
    """
    try:
        logger.info("Filling missing values in listing_dim.")
        listing_dim = listing_dim.na.fill({
                            "airbnbPropertyId": 0,
                            "listingTitle": "Unknown",
                            "propertyType": "Not Specified",
                            "listingType": "Not Specified",
                            "listingURL": "No URL",
                            "pictureURL": "No Image",
                            "createdDate": "1970-01-01",
                            "bedrooms": 1,
                            "bathrooms": 1,
                            "maxGuests": 2,
                            "instantBookable": False,
                            "petsAllowed": False,
                            "cancellationPolicy": "Not Specified"
                        })

        listing_dim = listing_dim.withColumn("listingTitle", regexp_replace(col("listingTitle"), r"'", ""))

        listing_dim = listing_dim.withColumn("petsAllowed", \
            when(col("petsAllowed") == "t", True)
            .when(col("petsAllowed") == "f", False).otherwise(False))\
        .withColumn("instantBookable", \
            when(col("instantBookable") == "t", True)\
            .when(col("instantBookable") == "f", False).otherwise(False))

        logger.info("Missing values in listing_dim filled successfully.")
        return listing_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in listing_dim: %s", e)
        return listing_dim




def fill_location_dim(location_dim: DataFrame) -> DataFrame:
    """
    Fill missing values in the location_dim DataFrame with default values.

    Args:
        location_dim (DataFrame): The input DataFrame containing location data.

    Returns:
        DataFrame: The DataFrame with missing values filled.
    """
    try:
        logger.info("Filling missing values in location_dim.")
        location_dim = location_dim.na.fill({
            "country": "Unknown",
            "state": "Unknown",
            "city": "Unknown",
            "zipcode": "000000",
            "latitude": 0.0,
            "longitude": 0.0,
            "exactLocation": False
        })

        location_dim = location_dim.withColumn(
            "exactLocation",
            when(col("exactLocation") == "t", True)
            .when(col("exactLocation") == "f", False)
            .otherwise(False)
        )
        logger.info("Missing values in location_dim filled successfully.")
        return location_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in location_dim: %s", e)
        return location_dim


def fill_host_dim(host_dim: DataFrame) -> DataFrame:
    """
    Fill missing values in the host_dim DataFrame with default values.

    Args:
        host_dim (DataFrame): The input DataFrame containing host data.

    Returns:
        DataFrame: The DataFrame with missing values filled.
    """
    try:
        logger.info("Filling missing values in host_dim.")
        host_dim = host_dim.na.fill({
                                "airbnbHostId": 0,
                                "hostListingCount": 0,
                                "superhost": False
                            })
        
        host_dim = host_dim.withColumn("superhost", when(col("superhost") == "t", True))\
                            .withColumn("superhost", when(col("superhost") == "f", False))

        logger.info("Missing values in host_dim filled successfully.")
        return host_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in host_dim: %s", e)
        return host_dim


def fill_review_dim(review_dim: DataFrame) -> DataFrame:
    """
    Fill missing values in the review_dim DataFrame with default values.

    Args:
        review_dim (DataFrame): The input DataFrame containing rating data.

    Returns:
        DataFrame: The DataFrame with missing values filled.
    """
    try:
        logger.info("Filling missing values in review_dim.")
        review_dim = review_dim.na.fill({
            "numberOfReviews": 0,
            "overallRating": 0.0,
            "communicationRating": 0.0,
            "accuracyRating": 0.0,
            "cleanlinessRating": 0.0,
            "checkinRating": 0.0,
            "locationRating": 0.0,
            "valueRating": 0.0
        })
        logger.info("Missing values in review_dim filled successfully.")
        return review_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in review_dim: %s", e)
        return review_dim


def process_amenities_dim(amenities_dim: DataFrame) -> DataFrame:
    """
    Processes the amenities dimension DataFrame by splitting, exploding, and cleaning the amenities column.

    Args:
    amenities_dim: The input DataFrame containing amenities data.

    Returns: The processed DataFrame with cleaned amenities column.
    """
    try:
        
        # Log the number of rows before processing
        logger.info("Number of rows before processing: %s", amenities_dim.count())

        # logger.info("Processing amenities dimension.") # exploded the number of rows into millions
        # amenities_dim = amenities_dim.withColumn("amenities", split(trim(col("amenities")), ",\\s*"))\
        #                              .withColumn("amenities", explode(col("amenities")))\
        #                              .withColumn("amenities", trim(col("amenities")))
        
        # amenities_dim = amenities_dim.withColumn("amenities", regexp_replace(col("amenities"), r"[\\[\\]']", ""))
        
        logger.info("Number of rows after processing: %s", amenities_dim.count())        
        logger.info("Amenities dimension processed successfully.")
        return amenities_dim
    except Exception as e:
        logger.error("Returnung df due to error processing amenities dimension: %s", e)
        return amenities_dim


def process_checkin_checkout_dim(checkin_checkout_dim: DataFrame) -> DataFrame:
    """
    Processes the check-in and check-out dimension DataFrame by cleaning and splitting the check-in time column.

    Args:
    checkin_checkout_dim: The input DataFrame containing check-in and check-out data.

    Returns: The processed DataFrame with cleaned and split check-in time columns.
    """
    try:
        logger.info("Processing check-in and check-out dimension.")

        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinTime", element_at(split(col("checkinTime"), ","), 1))


        # Replace 'After ' with an empty string
        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinTime", regexp_replace(col("checkinTime"), r"After\s", ""))

        # Split check-in time into start and end times
        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinStartTime", split(col("checkinTime"), "-")[0])\
                                                .withColumn("checkinEndTime", split(col("checkinTime"), "-")[1])\
                                                .drop("checkinTime")

        # Handle cases where check-in is "Flexible"
        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinEndTime", 
                                                    when(col("checkinStartTime") == "Flexible", "12:00 AM")
                                                    .otherwise(col("checkinEndTime")))

        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinStartTime", 
                                                    regexp_replace(col("checkinStartTime"), r"Flexible", "12:00 AM"))
                
        logger.info("Check-in and check-out dimension processed successfully.")
        return checkin_checkout_dim
    except Exception as e:
        logger.error("Error processing check-in and check-out dimension: %s", e)
        raise



def fill_checkin_checkout_dim(checkin_checkout_dim: DataFrame) -> DataFrame:
    """
    Fills missing values in the check-in and check-out dimension DataFrame with most frequent values

    Args:
    checkin_checkout_dim: The input DataFrame containing check-in and check-out data.

    Returns: The DataFrame with missing values filled.
    """
    try:
        logger.info("Getting most frequent value for checkin_checkout_dim")
        checkout_mode = (checkin_checkout_dim.groupBy("checkoutTime")\
                            .count()\
                            .orderBy(desc("count"))\
                            .filter(col("checkoutTime")\
                            .isNotNull()).first())

        checkin_start_mode = (checkin_checkout_dim.groupBy("checkinStartTime")\
                                .count()\
                                .orderBy(desc("count"))\
                                .filter(col("checkinStartTime")\
                                .isNotNull()).first())
        
        checkin_end_mode = (checkin_checkout_dim.groupBy("checkinEndTime")\
                                .count()\
                                .orderBy(desc("count"))\
                                .filter(col("checkinEndTime")\
                                .isNotNull()).first())

        # extract the mode from the array
        checkout_mode = checkout_mode[0]
        checkin_start_mode = checkin_start_mode[0]
        checkin_end_mode = checkin_end_mode[0]

        logger.info("Most frequent values - checkout_mode: %s, checkin_start_mode: %s, checkin_end_mode: %s",
                checkout_mode, checkin_start_mode, checkin_end_mode)

        # fil missing values
        logger.info("Filling missing values in checkin_checkout_dim")
        checkin_checkout_dim = checkin_checkout_dim.na.fill({
            "checkoutTime": checkout_mode,
            "checkinStartTime": checkin_start_mode,
            "checkinEndTime": checkin_end_mode,
            "minimumStay": "1"
        })

        # Convert time columns to the desired format
        logger.info("Converting time columns to the desired format in checkin_checkout_dim")
        checkin_checkout_dim = checkin_checkout_dim.withColumn("checkinStartTime", \
                                                    from_unixtime(unix_timestamp(col("checkinStartTime"), "hh:mm a"), "HH:mm"))\
                                                  .withColumn("checkinEndTime", \
                                                    from_unixtime(unix_timestamp(col("checkinEndTime"), "hh:mm a"), "HH:mm"))\
                                                  .withColumn("checkoutTime", \
                                                    from_unixtime(unix_timestamp(col("checkoutTime"), "hh:mm a"), "HH:mm"))\
                                                  .withColumn("minimumStay", \
                                                    col("minimumStay").cast("int"))
        logger.info("Missing values in check-in and check-out dimension filled successfully.")
        return checkin_checkout_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in check-in and check-out dimension: %s", e)
        return checkin_checkout_dim




def fill_pricing_dim(pricing_dim: DataFrame) -> DataFrame:

    """
    Fill missing values in the pricing_dim DataFrame with default values.

    Args:
        pricing_dim (DataFrame): The input DataFrame containing pricing data.

    Returns:
        DataFrame: The DataFrame with missing values filled.
    """
    try:
        logger.info("Filling missing values in pricing_dim.")
        pricing_dim = pricing_dim.na.fill({
            "currencyNative": "0.0",
            "cleaningFeeUSD": 0.0,
            "cleaningFeeNative": 0.0,
            "extraPeopleFeeUSD": 0.0,
            "extraPeopleFeeNative": 0.0,
            "averageDailyRateUSD": 0.0,
            "averageDailyRateNative": 0.0
        })
        logger.info("Missing values in pricing_dim filled successfully.")
        return pricing_dim
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in pricing_dim: %s", e)
        return pricing_dim
    
    


def fill_booking_fact(booking_fact: DataFrame) -> DataFrame:
    """
    Fill missing values in the booking_fact DataFrame with default values 
    and convert specific columns to boolean.

    Args:
        booking_fact (DataFrame): The input DataFrame containing booking data.

    Returns:
        DataFrame: The DataFrame with missing values filled and specific columns
        converted to boolean.
    """
    try:
        logger.info("Filling missing values in booking_fact.")      
        booking_fact = booking_fact.na.fill({
            "occupancyRateLtm": 0.0,
            "numberOfBookingsLtm": 0,
            "numberOfBookingsLtmObservedMonth": 0,
            "instantBookEnabled": False,
            "countReservationDaysLtm": 0,
            "countavailableDaysLtm": 0,
            "annualRevenueLtmUSD": 0.0,
            "annualRevenueLtmNative": 0.0
        })

        return booking_fact
    except Exception as e:
        logger.error("Returnung df due to error filling missing values in booking_fact dimension: %s", e)
        return booking_fact


# write dynamicframes to s3
def write_to_s3(df: DataFrame, folder_name: str, df_name: str) -> None:
    """
    Writes a dynamic frame to s3
  
    Args:
    df: The dynamic frame to write to s3
    df_name: The name of the dynamic frame
    
    Returns: None
    """
    try:
        logger.info("Writing %s to S3.", df_name)
        dyf = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        sink = glueContext.getSink(
                    path=f"s3://airbnb-listings-bucket/{folder_name}/{df_name}.csv",
                    connection_type="s3",
                    updateBehavior="UPDATE_IN_DATABASE",
                    partitionKeys=[],
                    compression="gzip",
                    transformation_ctx=f"{df_name}_csv_sink")
        sink.setFormat("csv")
        sink.writeFrame(dyf)
        logger.info("Data written to %s successfully.", df_name)
    except Exception as e:
        logger.error("Error writing %s to S3: %s", df_name, e)
        raise
    


# Call the main  
def main():

    # create dynamicframes for all the cities
    dubai = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/DubaiData.csv")
    london  = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/LondonData.csv")
    miami = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/MiamiData.csv")
    newYorkCity = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/NYCData.csv")
    sanFransisco = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/SanFransiscoData.csv")
    sydney = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/SydneyData.csv")
    tokyo =  create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/TokyoData.csv")
    toronto  = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/TorontoData.csv")
    losAngeles = create_dynamic_frame_from_csv("s3://airbnb-listings-bucket/raw-data/LAData.csv")
    
    
    # apply mappings to all the cities
    mapped_losAngeles= apply_mapping(losAngeles)
    mapped_london = apply_mapping(london)
    mapped_newYorkCity = apply_mapping(newYorkCity)
    mapped_sanFransisco = apply_mapping(sanFransisco)
    mapped_sydney= apply_mapping(sydney)
    mapped_tokyo = apply_mapping(tokyo)
    mapped_miami = apply_mapping(miami)
    mapped_toronto = apply_mapping(toronto)
    mapped_dubai = apply_mapping(dubai)

        
    
    # Convert dynamic frame to spark dataframe
    losAngeles_df = mapped_losAngeles.toDF()
    london_df = mapped_london.toDF() 
    miami_df = mapped_miami.toDF() 
    newYorkCity_df = mapped_newYorkCity.toDF() 
    sanFransisco_df = mapped_sanFransisco.toDF() 
    sydney_df = mapped_sydney.toDF() 
    tokyo_df = mapped_tokyo.toDF()
    dubai_df = mapped_dubai.toDF() 
    toronto_df = mapped_toronto.toDF()
    
    
    # drop columns
    losAngeles_df_step1_cleaned = drop_duplicates_and_common_missing_values(losAngeles_df)
    dubai_df_step1_cleaned = drop_duplicates_and_common_missing_values(dubai_df)
    london_df_step1_cleaned = drop_duplicates_and_common_missing_values(london_df)
    newYorkCity_df_step1_cleaned = drop_duplicates_and_common_missing_values(newYorkCity_df)
    sanFransisco_df_step1_cleaned = drop_duplicates_and_common_missing_values(sanFransisco_df)
    sydney_df_step1_cleaned = drop_duplicates_and_common_missing_values(sydney_df)
    toronto_df_step1_cleaned = drop_duplicates_and_common_missing_values(toronto_df)
    tokyo_df_step1_cleaned = drop_duplicates_and_common_missing_values(tokyo_df)
    miami_df_step1_cleaned = drop_duplicates_and_common_missing_values(miami_df)
   
    
    
    # remove emojis from all the dataframes
    tokyo_df_step2_cleaned = remove_emojis_from_df(tokyo_df_step1_cleaned)
    dubai_df_step2_cleaned = remove_emojis_from_df(dubai_df_step1_cleaned)
    losAngeles_df_step2_cleaned = remove_emojis_from_df(losAngeles_df_step1_cleaned)
    london_df_step2_cleaned = remove_emojis_from_df(london_df_step1_cleaned)
    newYorkCity_df_step2_cleaned = remove_emojis_from_df(newYorkCity_df_step1_cleaned)
    sanFransisco_df_step2_cleaned = remove_emojis_from_df(sanFransisco_df_step1_cleaned)
    sydney_df_step2_cleaned = remove_emojis_from_df(sydney_df_step1_cleaned)
    toronto_df_step2_cleaned = remove_emojis_from_df(toronto_df_step1_cleaned)
    miami_df_step2_cleaned = remove_emojis_from_df(miami_df_step1_cleaned)
    
    
    # Standardise columns for all the dataframes
    dubai_df_step3_cleaned = standardised_columns(df=dubai_df_step2_cleaned, country = "AE", city = "Dubai", state = "Dubai", currencyNative= "AED" )
    losAngeles_df_step3_cleaned = standardised_columns(df=losAngeles_df_step2_cleaned, country = "US", state = "Los Angeles", currencyNative= "USD")
    london_df_step3_cleaned = standardised_columns(df=london_df_step2_cleaned ,  country = "GB", state = "UK",  currencyNative= "GBP")
    miami_df_step3_cleaned = standardised_columns(df=miami_df_step2_cleaned, country = "US",state = "Florida",  currencyNative= "GBP" )
    newYorkCity_df_step3_cleaned = standardised_columns(df=newYorkCity_df_step2_cleaned, country = "US",  currencyNative= "USD" )
    sanFransisco_df_step3_cleaned = standardised_columns(df=sanFransisco_df_step2_cleaned,  country = "US", state = "California", city = "San Fransisco", currencyNative= "USD" )
    sydney_df_step3_cleaned = standardised_columns(df=sydney_df_step2_cleaned, country = "AU", state = "New South Wales", currencyNative= "AUD" )
    tokyo_df_step3_cleaned = standardised_columns(df=tokyo_df_step2_cleaned, country = "JP", state = "Tokyo", currencyNative= "JPY" )
    toronto_df_step3_cleaned = standardised_columns(df=toronto_df_step2_cleaned,country = "CA", state = "Ontario", currencyNative= "CAD" )
    
    
    # create id columns for dimension (review, amenities, booking, listing, checkinCheckout) in df 
    losAngeles_df_step4_cleaned = create_id_columns("losAngeles", losAngeles_df_step3_cleaned)
    london_df_step4_cleaned = create_id_columns("london", london_df_step3_cleaned)
    miami_df_step4_cleaned = create_id_columns("miami", miami_df_step3_cleaned)
    newYorkCity_df_step4_cleaned = create_id_columns("newYorkCity", newYorkCity_df_step3_cleaned)
    sanFransisco_df_step4_cleaned = create_id_columns("sanFransisco", sanFransisco_df_step3_cleaned)
    sydney_df_step4_cleaned = create_id_columns("sydney", sydney_df_step3_cleaned)
    tokyo_df_step4_cleaned = create_id_columns("tokyo", tokyo_df_step3_cleaned)
    toronto_df_step4_cleaned = create_id_columns("toronto", toronto_df_step3_cleaned)
    dubai_df_step4_cleaned = create_id_columns("dubai", dubai_df_step3_cleaned)
    
    
    # check and fix columns for all the dataframes
    losAngeles_df_step4_cleaned = check_and_fix_columns(losAngeles_df_step4_cleaned)
    london_df_step4_cleaned = check_and_fix_columns(london_df_step4_cleaned)
    miami_df_step4_cleaned = check_and_fix_columns(miami_df_step4_cleaned)
    newYorkCity_df_step4_cleaned = check_and_fix_columns(newYorkCity_df_step4_cleaned)
    sanFransisco_df_step4_cleaned = check_and_fix_columns(sanFransisco_df_step4_cleaned)
    sydney_df_step4_cleaned = check_and_fix_columns(sydney_df_step4_cleaned)
    tokyo_df_step4_cleaned = check_and_fix_columns(tokyo_df_step4_cleaned)
    toronto_df_step4_cleaned = check_and_fix_columns(toronto_df_step4_cleaned)
    dubai_df_step4_cleaned = check_and_fix_columns(dubai_df_step4_cleaned)

    
    # define dimension columns
    listing_columns, host_columns, location_columns, amenities_columns, review_columns, checkin_checkout_columns, pricing_columns, booking_columns = define_dimension_columns()


    # create dimension tables
    listing_dim = (losAngeles_df_step4_cleaned.select(listing_columns)
                .union(london_df_step4_cleaned.select(listing_columns))\
                .union(miami_df_step4_cleaned.select(listing_columns))\
                .union(newYorkCity_df_step4_cleaned.select(listing_columns))\
                .union(sanFransisco_df_step4_cleaned.select(listing_columns))\
                .union(sydney_df_step4_cleaned.select(listing_columns))\
                .union(tokyo_df_step4_cleaned.select(listing_columns))  \
                .union(toronto_df_step4_cleaned.select(listing_columns))\
                .union(dubai_df_step4_cleaned.select(listing_columns)))
      
    
    
    host_dim = (losAngeles_df_step4_cleaned.select(host_columns)
                .union(london_df_step4_cleaned.select(host_columns))\
                .union(miami_df_step4_cleaned.select(host_columns))\
                .union(newYorkCity_df_step4_cleaned.select(host_columns))\
                .union(sanFransisco_df_step4_cleaned.select(host_columns))\
                .union(sydney_df_step4_cleaned.select(host_columns))\
                .union(tokyo_df_step4_cleaned.select(host_columns))\
                .union(toronto_df_step4_cleaned.select(host_columns))\
                .union(dubai_df_step4_cleaned.select(host_columns)))
    
    
    location_dim = (losAngeles_df_step4_cleaned.select(location_columns)
                .union(london_df_step4_cleaned.select(location_columns))\
                .union(miami_df_step4_cleaned.select(location_columns))\
                .union(newYorkCity_df_step4_cleaned.select(location_columns))\
                .union(sanFransisco_df_step4_cleaned.select(location_columns))\
                .union(sydney_df_step4_cleaned.select(location_columns))\
                .union(tokyo_df_step4_cleaned.select(location_columns))\
                .union(toronto_df_step4_cleaned.select(location_columns))\
                .union(dubai_df_step4_cleaned.select(location_columns)))
    
    
    amenities_dim = (losAngeles_df_step4_cleaned.select(amenities_columns)
                .union(london_df_step4_cleaned.select(amenities_columns))\
                .union(miami_df_step4_cleaned.select(amenities_columns))\
                .union(newYorkCity_df_step4_cleaned.select(amenities_columns))\
                .union(sanFransisco_df_step4_cleaned.select(amenities_columns))\
                .union(sydney_df_step4_cleaned.select(amenities_columns))\
                .union(tokyo_df_step4_cleaned.select(amenities_columns))\
                .union(toronto_df_step4_cleaned.select(amenities_columns))\
                .union(dubai_df_step4_cleaned.select(amenities_columns)))
    
    
    review_dim = (losAngeles_df_step4_cleaned.select(review_columns)
                .union(london_df_step4_cleaned.select(review_columns))\
                .union(miami_df_step4_cleaned.select(review_columns))\
                .union(newYorkCity_df_step4_cleaned.select(review_columns))\
                .union(sanFransisco_df_step4_cleaned.select(review_columns))\
                .union(sydney_df_step4_cleaned.select(review_columns))\
                .union(tokyo_df_step4_cleaned.select(review_columns))\
                .union(toronto_df_step4_cleaned.select(review_columns))\
                .union(dubai_df_step4_cleaned.select(review_columns)))
    
    
    
    checkin_checkout_dim  = (losAngeles_df_step4_cleaned.select(checkin_checkout_columns)
                .union(london_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(miami_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(newYorkCity_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(sanFransisco_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(sydney_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(tokyo_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(toronto_df_step4_cleaned.select(checkin_checkout_columns))\
                .union(dubai_df_step4_cleaned.select(checkin_checkout_columns)))
    

    pricing_dim = (losAngeles_df_step4_cleaned.select(pricing_columns)
                .union(london_df_step4_cleaned.select(pricing_columns))\
                .union(miami_df_step4_cleaned.select(pricing_columns))\
                .union(newYorkCity_df_step4_cleaned.select(pricing_columns))\
                .union(sanFransisco_df_step4_cleaned.select(pricing_columns))\
                .union(sydney_df_step4_cleaned.select(pricing_columns))\
                .union(tokyo_df_step4_cleaned.select(pricing_columns))\
                .union(toronto_df_step4_cleaned.select(pricing_columns))\
                .union(dubai_df_step4_cleaned.select(pricing_columns)))
    
    
    # partition the booking_fact dimension
    booking_fact = (losAngeles_df_step4_cleaned.select(booking_columns)
                .union(london_df_step4_cleaned.select(booking_columns))\
                .union(miami_df_step4_cleaned.select(booking_columns))\
                .union(newYorkCity_df_step4_cleaned.select(booking_columns))\
                .union(sanFransisco_df_step4_cleaned.select(booking_columns))\
                .union(sydney_df_step4_cleaned.select(booking_columns))\
                .union(tokyo_df_step4_cleaned.select(booking_columns))\
                .union(toronto_df_step4_cleaned.select(booking_columns))\
                .union(dubai_df_step4_cleaned.select(booking_columns))
                .repartition(10)
             )
    


    # fill dimension tables
    listing_dim_transformed = fill_listing_dim(listing_dim)
    location_dim_transformed = fill_location_dim(location_dim)
    host_dim_transformed = fill_host_dim(host_dim)
    amenities_dim_transformed = process_amenities_dim(amenities_dim)
    review_dim_transformed = fill_review_dim(review_dim)
    checkin_checkout_dim_transformed = process_checkin_checkout_dim(checkin_checkout_dim)
    checkin_checkout_dim_transformed = fill_checkin_checkout_dim(checkin_checkout_dim_transformed)
    pricing_dim_transformed = fill_pricing_dim(pricing_dim)
    booking_fact_transformed = fill_booking_fact(booking_fact)
    
    
    # write all the dynamicframes to s3
    write_to_s3(listing_dim_transformed, "listing", "listing-dim")
    write_to_s3(location_dim_transformed, "location", "location-dim")
    write_to_s3(host_dim_transformed, "host", "host-dim")
    write_to_s3(amenities_dim_transformed, "amenities", "amenities-dim")
    write_to_s3(review_dim_transformed, "review", "review-dim")
    write_to_s3(checkin_checkout_dim_transformed, "checkinout", "checkinout-dim")
    write_to_s3(pricing_dim_transformed, "pricing", "pricing-dim")
    write_to_s3(booking_fact_transformed, "booking", "booking-fact")


if __name__ == "__main__":
    try:
        main() 
    except Exception as e:
        logger.error("Job execution failed: %s", e)
        raise
    finally:
        if 'job' in locals():
            logger.info("Committing Glue job...")
            job.commit()
            logger.info("Glue job committed successfully.")


