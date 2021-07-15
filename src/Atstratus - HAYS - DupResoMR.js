/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/search', 'N/runtime', 'N/https', 'N/config'],
/**
 * @param {record} record
 * @param {search} search
 */
function(record, search, runtime, https, config) {
    /**
     * Marks the beginning of the Map/Reduce process and generates input data.
     *
     * @typedef {Object} ObjectRef
     * @property {number} id - Internal ID of the record instance
     * @property {string} type - Record type id
     *
     * @return {Array|Object|Search|RecordRef} inputSummary
     * @since 2015.1
     */
    function getInputData() {
        var customrecord_hays_eventSearchObj = search.create({
            type: "customrecord_hays_event",
            filters:
            [
               ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"], 
               "AND", 
               ["custrecord_hays_event_status","anyof","1","3"], 
               "AND", 
               ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"], 
               "AND", 
               ["internalid","anyof","589421"]  //Remove soon. This is just a sample.
            ],
            columns:
            [
               search.createColumn({
                  name: "id",
                  sort: search.Sort.ASC,
                  label: "ID"
               }),
               search.createColumn({name: "created", label: "Date Created"}),
               search.createColumn({name: "custrecord_hays_event_subsidiary", label: "Subsidiary"}),
               search.createColumn({name: "custrecord_hays_event_recordtype", label: "Record Type"}),
               search.createColumn({name: "custrecord_hays_event_status", label: "Status"}),
               search.createColumn({name: "custrecord_hays_event_errorcodelist", label: "Error Code List"})
            ]
        });
        var searchResultCount = customrecord_hays_eventSearchObj.runPaged().count;
        log.debug("customrecord_hays_eventSearchObj result count",searchResultCount);

        return customrecord_hays_eventSearchObj;
    }

    /**
     * Executes when the map entry point is triggered and applies to each key/value pair.
     *
     * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
     * @since 2015.1
     */
    function map(context) {
        var searchResult = JSON.parse(context.value);
        var internalid = searchResult.values['internalid'].value;

        //TODO
    }

    /**
     * Executes when the summarize entry point is triggered and applies to the result set.
     *
     * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
     * @since 2015.1
     */
    function summarize(summary) {
        log.audit('Summary Data', summary);
        log.audit({
            title: 'Usage units consumed', 
            details: summary.usage
        });
        log.audit({
            title: 'Concurrency',
            details: summary.concurrency
        });
        log.audit({
            title: 'Number of yields', 
            details: summary.yields
        });
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});
