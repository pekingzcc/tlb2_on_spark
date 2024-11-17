PAGE_VIEW_EVENT_TAG_KEYS = {
    "page_urlhost": "page_host",
    "page_urlpath": "page_path",
    "page_urlquery": "page_query",
    "page_referrer": "page_referrer",
    "page_title": "page_title",
    "refr_urlhost": "referrer_urlhost",
    "page_url": "page_url",
}

PERFORMANCE_CONTEXT_TAG_KEYS = {
    # "contexts.performance_context.st": "navigationStart",
    # "contexts.performance_context.le": "loadEventEnd",
    # "contexts.performance_context.dls": "domainLookupStart",
    # "contexts.performance_context.dle": "domainLookupEnd",
    # "contexts.performance_context.rss": "responseStart",
    # "contexts.performance_context.rse": "responseEnd",
    # "contexts.performance_context.lcp.rt": "renderTime",
}

SCREEN_VIEW_EVENT_TAG_KEYS = {
    # "contexts.mobile_context.osType": "os_type",
    # "contexts.android_app_loadtime.onResume": "android_app_loadtime_onResume",
    # "contexts.android_app_loadtime.appLoad": "android_app_loadtime_appLoad",
    # "contexts.ios_app_loadtime.userInteractable": "ios_user_interactable",
    # "contexts.ios_app_loadtime.loadedToMemory": "ios_loaded_to_memory",
}


NETWORK_REQUEST_TARGET_URL_KEY = "flatten_event_data.targetUrl"

# For K/V type, defined the key whose length is empty or greater than MAP_KEY_LEN_LIMIT as bad keys
MAP_KEY_LEN_LIMIT = 200

PAGE_ID_SPECIAL_CUSTOMER = ["1960180550"]

# key is the category name of the platform, and value is the exact names of platform
PLATFORM_CATEGORIES_MAPPING = {
    "web": ["web"],
    "mob": ["mob", "pc", "srv", "app", "tv", "cnsl", "iot"],
}

WEB_PLATFORMS = PLATFORM_CATEGORIES_MAPPING.get("web", [])
MOBILE_PLATFORMS = PLATFORM_CATEGORIES_MAPPING.get("mob", [])

CUSTOM_METADATA_KEY = "flatten_custom_tags"

ECO_EVENT_SOURCE_SCHEMA = [
    {"app_id?": "String"},
    {"client_id?": {"element": "Integer"}},
    # {"collector_tstamp?": "String"},
    # {"dvce_sent_tstamp?": "String"},
    # {"dvce_created_tstamp?": "String"},
    {
        "contexts?": [
            {
                "PerformanceTiming?": [
                    {"loadEventEnd?": "Number"},
                    {"navigationStart?": "Number"},
                ]
            },
            {"alt?": [{"le?": "Number"}, {"ls?": "Number"}]},
            {
                "android_app_loadtime?": [
                    {"appLoad?": "Number"},
                    {"onResume?": "Number"},
                ]
            },
            {
                "android_screen_loadtime?": [
                    {"onCreate?": "Number"},
                    {"onResume?": "Number"},
                ]
            },
            {"application?": [{"build?": "String"}, {"version?": "String"}]},
            {"bundle_info?": [{"info?": "String"}]},
            {"clid_schema?": [{"clid?": "String"}]},
            # {
            #     "geo_schema?": [
            #         {"asn?": "Integer"},
            #         {"connType?": "Integer"},
            #         {"encodeGeoCity?": "Integer"},
            #         {"encodeGeoCountry?": "Integer"},
            #         {"encodeGeoState?": "Integer"},
            #         {"errorFlags?": "Integer"},
            #         {"isp?": "Integer"},
            #         {"latitudeTimesThousand?": "Integer"},
            #         {"longitudeTimesThousand?": "Integer"},
            #         {"postalCode?": "String"},
            #         {"timezoneOffsetMins?": "Integer"},
            #     ]
            # },
            {
                "ios_app_loadtime?": [
                    {"loadedToMemory?": "Number"},
                    {"userInteractable?": "Number"},
                ]
            },
            {
                "ios_screen_loadtime?": [
                    {"screenInteractable?": "Number"},
                    {"screenLoadedToMemory?": "Number"},
                ]
            },
            {"mobile_context?": [{"osType?": "String"}]},
            {
                "performance_context?": [
                    {"dle?": "Number"},
                    {"dls?": "Number"},
                    {"lcp?": [{"rt?": "Number"}]},
                    {"le?": "Number"},
                    {"rse?": "Number"},
                    {"rss?": "Number"},
                    {"st?": "Number"},
                ]
            },
            {"screen?": [{"id?": "String"}, {"name?": "String"}]},
            {"slt?": [{"le?": "Number"}, {"ls?": "Number"}]},
        ]
    },
    # {"customer_id?": "Integer"},
    # {"etl_tstamp?": "String"},
    # {"event_category?": "String"},
    # {"event_id?": "String"},
    {"event_name?": "String"},
    # {"event_vendor?": "String"},
    # {"event_version?": "String"},
    {"flatten_custom_tags?": {"value": "String"}},
    {"flatten_event_data?": {"value": "String"}},
    # {"geo_city?": "String"},
    # {"ip_domain?": "String"},
    # {"ip_netspeed?": "String"},
    {
        "m3dv?": [
            {"br?": "String"},
            {"brv?": "String"},
            {"cat?": "String"},
            {"fw?": "String"},
            {"fwv?": "String"},
            {"hwt?": "String"},
            {"mdl?": "String"},
            {"mnf?": "String"},
            {"mrk?": "String"},
            {"n?": "String"},
            {"os?": "String"},
            {"osf?": "String"},
            {"osv?": "String"},
            {"vnd?": "String"},
        ]
    },
    {"page_referrer?": "String"},
    {"page_title?": "String"},
    {"page_url?": "String"},
    {"page_urlfragment?": "String"},
    {"page_urlhost?": "String"},
    {"page_urlpath?": "String"},
    {"page_urlport?": "String"},
    {"page_urlquery?": "String"},
    {"platform?": "String"},
    {"refr_urlhost?": "String"},
    {"refr_urlpath?": "String"},
    {"refr_urlport?": "String"},
    {
        "unstruct_event?": [
            # {"screen_view?": [{"previousId?": "String"}, {"previousName?": "String"}]},
            {"raw_event?": [{"name?": "String"}]},
            # {"conviva_video_events?": [{"name?": "String"}]},
            # {
            #     "network_request?": [
            #         {"duration?": "Number"},
            #         {"responseStatusCode?": "Number"},
            #         {"webResourceTiming?": [{"responseStatus?": "Number"}]},
            #     ]
            # },
        ]
    },
    # {"user_id?": "String"},
    # {"user_ipaddress?": "String"},
    # {"useragent?": "String"},
    # {"v_tracker?": "String"},
]

from pytimeline.dagfunction.function_pool import DagFunctionPool
from pytimeline.dagfunction.context import DagFunctionContext
from pytimeline.dagfunction.timeline import Timeline as DagFunctionTimeline

def build_preprocessing_func(ctx: DagFunctionContext):
    ctx.eventNameFromSensor = ctx.rawEvents.get_or_drop("event_name")
    ctx.eventNameBeforeMapping = transform_event_name(ctx)
    ctx.unstructEventData = build_unstruct_event_data(ctx)
    ctx.eventTagKeyValueJSON = build_event_tag_key_values(ctx)
    ctx.pageScreenId = build_page_screen_id(ctx)
    ctx.networkRequestDuration = build_network_request_duration(ctx)

    res = DagFunctionTimeline.make_struct(
        unstructEventData=ctx.unstructEventData,
        eventNameFromSensor=ctx.eventNameFromSensor,
        eventNameBeforeMapping=ctx.eventNameBeforeMapping,
        eventTagKeyValueJSON=ctx.eventTagKeyValueJSON,
        pageScreenId=ctx.pageScreenId,
        networkRequestDuration=ctx.networkRequestDuration
    )
    return res


def transform_event_name(ctx: DagFunctionContext):
    res = DagFunctionTimeline.case_or_else(
        (
            ctx.eventNameFromSensor == "raw_event",
            ctx.rawEvents.get_or_else("unstruct_event.raw_event.name", ""),
        ),
        (ctx.eventNameFromSensor == "conviva_video_events", "conviva_video_events"),
        ctx.eventNameFromSensor.to_string("conviva_{}"),
    )
    return res


def build_unstruct_event_data(ctx: DagFunctionContext):
    ctx.flatten_event_data = ctx.rawEvents.get_or_drop("flatten_event_data")
    res = DagFunctionTimeline.make_struct(
        screen_view=(ctx.eventNameFromSensor == "screen_view").if_or_drop(
            ctx.flatten_event_data
        ),
        conviva_video_events=(
                ctx.eventNameFromSensor == "conviva_video_events"
        ).if_or_drop(ctx.flatten_event_data),
        raw_event=(ctx.eventNameFromSensor == "raw_event").if_or_drop(
            ctx.flatten_event_data
        ),
        network_request=(ctx.eventNameFromSensor == "network_request").if_or_drop(
            ctx.flatten_event_data
        ),
    )
    return res


def build_event_tag_key_values(ctx: DagFunctionContext):
    columns = {
        "platform": ctx.rawEvents["platform"],
        "app_id": ctx.rawEvents["app_id"],
        "os_type": ctx.rawEvents["contexts.mobile_context.osType"],
        "m3dv.br": ctx.rawEvents["m3dv.br"],
        "m3dv.brv": ctx.rawEvents["m3dv.brv"],
        "m3dv.cat": ctx.rawEvents["m3dv.cat"]
    }
    for key, value in PAGE_VIEW_EVENT_TAG_KEYS.items():
        columns[value] = (
                (ctx.eventNameBeforeMapping == "conviva_page_view")
                & (ctx.rawEvents[key] != "")
        ).if_or_drop(ctx.rawEvents[key].to_string())

    for field_key, tag_key in SCREEN_VIEW_EVENT_TAG_KEYS.items():
        columns[tag_key] = (
                ctx.eventNameBeforeMapping == "conviva_screen_view"
        ).if_or_drop(ctx.rawEvents.get_or_drop(field_key).to_string())

    for key, value in PERFORMANCE_CONTEXT_TAG_KEYS.items():
        columns[value] = ctx.rawEvents.get_or_drop(key).to_string()

    ctx.url_map = (ctx.eventNameBeforeMapping == "conviva_network_request").if_or_drop(
        ctx.rawEvents.get_or_drop(NETWORK_REQUEST_TARGET_URL_KEY).parse_url()
    )

    columns["network_request_url_path"] = ctx.url_map["path"].filter(
        ctx.url_map.get_or_drop("path") != ""
    )

    columns["network_request_url_query"] = (
        ctx.url_map["query"].filter(ctx.url_map["query"] != "").to_string("?{}")
    )

    columns["network_request_url_scheme"] = ctx.url_map["scheme"].filter(
        ctx.url_map["scheme"] != ""
    )

    columns["network_request_url_host"] = ctx.url_map["host"].filter(
        ctx.url_map["host"] != ""
    )

    default_port = DagFunctionTimeline.case_or_drop(
        (ctx.url_map.get_or_drop("scheme") == "http", "80"),
        (ctx.url_map.get_or_drop("scheme") == "https", "443"),
        (ctx.url_map.get_or_drop("scheme") == "ftp", "21"),
    )

    columns["network_request_url_port"] = DagFunctionTimeline.case_or_else(
        (ctx.url_map.path_not_exist("port"), default_port),
        (ctx.url_map["port"].to_string() == "0", default_port),
        ctx.url_map.get_or_drop("port").to_string(),
    )

    columns["network_request_url_fragment"] = ctx.url_map["fragment"].filter(
        ctx.url_map["fragment"] != ""
    )

    columns["mobile_bundle_info_info"] = ctx.rawEvents.get_or_drop(
        "contexts.bundle_info.info"
    )

    # Remove invalid keys (empty or long keys)
    # Currently, there's no empty or long keys from event tags, but left it to get rid of potential issues in future
    columns = {
        k: v for k, v in columns.items() if k.strip() and len(k) <= MAP_KEY_LEN_LIMIT
    }

    event_tags_struct = DagFunctionTimeline.make_map(**columns)

    all_event_tags_structs = [
        event_tags_struct,
        ctx.rawEvents.get_or_drop("flatten_event_data"),
        ctx.rawEvents.get_or_drop("flatten_custom_tags"),
    ]

    return DagFunctionTimeline.merge_maps(*all_event_tags_structs)


def build_page_screen_id(ctx: DagFunctionContext) -> DagFunctionTimeline:
    """
    1. Platform is web and not page view, then check if customer is a special customer
        1.1 if yes, hash `page_urlhost/page_urlpath?page_urlquery#page_urlfragment` as pageScreenId
        1.2 if no,  hash `page_urlhost/page_urlpath` as pageScreenId
    2. Platform is mob, then pick `contexts.screen.id` as pageScreenId
    """
    platform = ctx.rawEvents["platform"]
    page_urlhost = ctx.rawEvents["page_urlhost"].filter(
        ctx.rawEvents["page_urlhost"] != ""
    )
    page_urlpath = ctx.rawEvents["page_urlpath"]
    page_urlquery = ctx.rawEvents["page_urlquery"]
    page_urlfragment = ctx.rawEvents["page_urlfragment"]
    not_page_view = ctx.eventNameBeforeMapping != "conviva_page_view"

    ctx.beforeHash = DagFunctionTimeline.format("{}/{}", page_urlhost, page_urlpath)

    res = DagFunctionTimeline.case_or_drop(
        (platform.is_in(*MOBILE_PLATFORMS), ctx.rawEvents["contexts.screen.id"]),
        (
            platform.is_in(*WEB_PLATFORMS) & not_page_view,
            DagFunctionTimeline.format("{}/{}", page_urlhost, page_urlpath)
            .hash_value()
            .to_string(),
        ),
    )
    return res


def build_network_request_duration(ctx: DagFunctionContext):
    ctx.duration = (ctx.eventNameBeforeMapping == "conviva_network_request").if_or_drop(
        ctx.rawEvents["flatten_event_data.duration"].parse_float_or_drop()
    )

    result = DagFunctionTimeline.case_or_else(
        ((ctx.duration >= 0.0) & (ctx.duration <= 90000.0), ctx.duration), -1.0
    )
    return result


def stateless_processing_func_pool(
        func_pool: DagFunctionPool
):

    func_pool.schema_pool.add_schemas(rawEvents=ECO_EVENT_SOURCE_SCHEMA)

    ctx: DagFunctionContext = func_pool.new_dag_function("main", rawEvents="rawEvents")

    # Preprocess
    ctx.preprocOutput = func_pool.define_dag_function(
        func_name="preprocess",
        func_body=build_preprocessing_func,
        rawEvents=ctx.rawEvents,
    )

    main_ctx = func_pool["main"]

    # combine all
    main_ctx.output = ctx.rawEvents.update_struct(
        unstructEventData=ctx.preprocOutput['unstructEventData'],
        eventNameFromSensor=ctx.preprocOutput['eventNameFromSensor'],
        eventName=ctx.preprocOutput['eventNameBeforeMapping'],
        eventTagKeyValueJSON=ctx.preprocOutput['eventTagKeyValueJSON'],
        pageScreenId=ctx.preprocOutput['pageScreenId'],
        networkRequestDuration=ctx.preprocOutput['networkRequestDuration']
    )
    main_ctx.add_outputs(main_ctx.output)
    return func_pool


import os.path

from pytimeline.dag.dag import Dag
from pytimeline.timeline_request_config.inputs import InputSource
from pytimeline.dag.timeline import Timeline as DagTimeline
from pytimeline.timeline_request_config.context import TimelineRequestContext


SESSION_EXPIRY_SECONDS = 90


def build_session_metrics(dag: Dag):
    dag.globalEventIndex = dag.rawEvents.count_each_event()
    dag.sessionStartEvents = dag.hasEvents.state_change_events()
    dag.isSessionEnd = dag.hasEvents.lookahead("1ns").is_null()
    dag.sessionStartTimeMs = (
            dag.rawEvents.event_time_nano().first_except().replace_null()
            / DagTimeline.constant_int(1000000)
    )
    dag.sessionDuration = dag.sessionStartEvents.duration_since_last_event()
    dag.sessionEndEvents = dag.isSessionEnd.state_change_events().filter(True)

    dag.eventTime = dag.rawEvents.event_time_nano()
    dag.eventTimePrior = dag.eventTime.prior_event_with_none()
    dag.durationSinceLastEvent = dag.eventTimePrior.is_null().if_opr(
        DagTimeline.constant_int(0),
        dag.eventTime - dag.eventTimePrior.replace_null()
    ) / DagTimeline.constant_int(1000000)

    return dag.rawEvents.update_struct(
        clientId=dag.clientIdByDot,
        globalEventIndex=dag.globalEventIndex,
        sessionDuration=dag.sessionDuration.evaluate_at(dag.rawEvents).round(),
        sessionStartTimeMs=dag.sessionStartTimeMs.evaluate_at(dag.rawEvents),
        durationSinceLastEvent=dag.durationSinceLastEvent
    )


def build_dag_with_input(input_file_path: str) -> TimelineRequestContext:
    ctx: TimelineRequestContext = TimelineRequestContext()
    dag = ctx.dag

    stateless_processing_func_pool(ctx.functions)

    input_source = InputSource(
        format="snowplow",
        schema="String",
        source='event-file-source',
        session_id_paths="contexts.clid_schema.clid",
        event_time_path="dvce_created_tstamp",
        sent_time_path="dvce_sent_tstamp",
        observed_time_path="collector_tstamp",
        input_name="aa_raw_events",
        pre_process_by="main",
        file_path=input_file_path,
        function_pool=ctx.functions,
        additional_configs={
            "client-id-path": "contexts.clid_schema.clid",
            "transformed-client-id-path": "client_id",
            "preprocessors": {
                "pre-deser": [],
                "post-deser": ["MetadataEnricher", "ClientIdTransformer"]
            }
        }
    )
    dag.allEvents = ctx.load_as_timeline(input_source=input_source)
    dag.hasEvents = dag.allEvents.set_all_events_to(True).has_been_true(
        sliding_window_secs=SESSION_EXPIRY_SECONDS
    )

    dag.rawEvents = dag.allEvents.filter(dag.allEvents["eventName"].not_in("conviva_periodic_heartbeat", "conviva_page_ping"))
    dag.clientIdArray = dag.rawEvents.get_or_else("client_id", [1, 1, 1, 1])
    dag.clientIdByDot = dag.clientIdArray.array_to_string(".")

    dag.clock = ctx.schedule(interval="1m", offset=-1)

    dag.outputEventsAll = dag.hasEvents.as_scope(
        dag, build_session_metrics, keys_with_no_scope=[False]
    )

    dag.outputEvents = dag.outputEventsAll[[
        'globalEventIndex',
        'sessionStartTimeMs',
        'sessionDuration',
        'clientId', 'eventName', dag.outputEventsAll['event_name'].alias("origEventName"),
        'durationSinceLastEvent',
        dag.outputEventsAll['eventTagKeyValueJSON'].alias("eventTags")
    ]]
    return ctx
