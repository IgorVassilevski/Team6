package org.elasticsearch.index;

/**
 * Created by hui on 2016-12-03.
 */
public class conflictsDescription {

    public static String fleiddate  = "mapper [%1$s] is used by multiple types. Set update_all_types to true to update [fielddata] across all types.";

    public static String fielddataMinFrequency = "mapper [%1$s] is used by multiple types. Set update_all_types to true to update "
            + "[fielddata_frequency_filter.min] across all types.";

    public static String fielddataMaxFrequency = "mapper [%1$s] is used by multiple types. Set update_all_types to true to update "
            + "[fielddata_frequency_filter.max] across all types.";


    public static String fielddataMinSegmentSize = "mapper [%1$s] is used by multiple types. Set update_all_types to true to update "
            + "[fielddata_frequency_filter.min_segment_size] across all types.";

}
