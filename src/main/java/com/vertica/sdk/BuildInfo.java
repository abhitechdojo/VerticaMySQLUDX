package com.vertica.sdk;

import com.vertica.sdk.VerticaBuildInfo;

/**
 * Created by abhishek.srivastava on 1/5/16.
 */

public class BuildInfo {
    public static final String VERTICA_BUILD_ID_Brand_Name       = "Vertica Analytic Database";
    public static final String VERTICA_BUILD_ID_Brand_Version    = "v7.2.1-0";
    public static final String VERTICA_BUILD_ID_SDK_Version      = "7.2.1";
    public static final String VERTICA_BUILD_ID_Codename         = "Excavator";
    public static final String VERTICA_BUILD_ID_Date             = "Mon Nov 16 15:28:29 2015";
    public static final String VERTICA_BUILD_ID_Machine          = "build-centos6";
    public static final String VERTICA_BUILD_ID_Branch           = "releases/VER_7_2_RELEASE_BUILD_1_0_20151116";
    public static final String VERTICA_BUILD_ID_Revision         = "177918";
    public static final String VERTICA_BUILD_ID_Checksum         = "1cbc1de500da229289eb94bab338a09f";

    public static VerticaBuildInfo get_vertica_build_info() {
        VerticaBuildInfo vbi = new VerticaBuildInfo();
        vbi.brand_name      = BuildInfo.VERTICA_BUILD_ID_Brand_Name;
        vbi.brand_version   = BuildInfo.VERTICA_BUILD_ID_Brand_Version;
        vbi.sdk_version     = BuildInfo.VERTICA_BUILD_ID_SDK_Version;
        vbi.codename        = BuildInfo.VERTICA_BUILD_ID_Codename;
        vbi.build_date      = BuildInfo.VERTICA_BUILD_ID_Date;
        vbi.build_machine   = BuildInfo.VERTICA_BUILD_ID_Machine;
        vbi.branch          = BuildInfo.VERTICA_BUILD_ID_Branch;
        vbi.revision        = BuildInfo.VERTICA_BUILD_ID_Revision;
        vbi.checksum        = BuildInfo.VERTICA_BUILD_ID_Checksum;
        return vbi;
    }
}