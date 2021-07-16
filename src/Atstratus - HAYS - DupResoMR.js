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
        var haysEventID = currentScript.getParameter({
            name: 'custscript_haysevent_id'
        });
        var customrecord_hays_eventSearchObj;

        if(haysEventID){
            customrecord_hays_eventSearchObj = search.create({
                type: "customrecord_hays_event",
                filters:
                [
                   ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"], 
                   "AND", 
                   ["custrecord_hays_event_status","anyof","3"], 
                   "AND", 
                   ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"], 
                   "AND", 
                   ["internalid","anyof",haysEventID]   //For manual processing
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
            log.audit("Manual Processing - START", 'To process: ' + searchResultCount);
        }
        else{
            customrecord_hays_eventSearchObj = search.create({
                type: "customrecord_hays_event",
                filters:
                [
                   ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"], 
                   "AND", 
                   ["custrecord_hays_event_status","anyof","3"], 
                   "AND", 
                   ["custrecord_hays_event_errorcodelist","contains","CANT_MODIFY_SUB"]
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
            log.audit("Automatic Processing - START", 'To process: ' + searchResultCount);
        }

        return customrecord_hays_eventSearchObj;
    }

    /**
     * Executes when the map entry point is triggered and applies to each key/value pair.
     *
     * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
     * @since 2015.1
     */
    function map(context) {
        try{
            var searchResult = JSON.parse(context.value);
            //log.audit('searchResult', searchResult);
            var internalid = searchResult.id;
            //log.audit('internalid', internalid);
    
            //Load the HAYS Event based on the internal ID of the search
            var haysEvent = record.load({
                type: 'customrecord_hays_event',
                id: internalid,
            });
    
            //Parse the payload to JSON
            var haysJSON = haysEvent.getValue({ fieldId: 'custrecord_hays_event_payload'});
            haysJSON = JSON.parse(haysJSON);
            var customerExtId = '';
    
            //Try and find the 'customer' recordtype from the JSON values
            //The haysJSON variable will be an array of an object
            for(var x = 0; x < haysJSON.length; x++){
                if(haysJSON[x].recordtype == 'customer' && haysJSON[x].externalid){
                    customerExtId = haysJSON[x].externalid;
                    
                    //There's probably one customer only. So once done, terminate the loop.
                    x = haysJSON.length;
                }
            }
            log.audit('customerExtId being processed', customerExtId);
    
            //Now that we have the customer external ID, move on to next search.
            //This time, we try and find the entity with the referenced external ID
            var duplicateCustomer;
            var duplicateCustomerExtId;
            var addedText;
            var customerSearchObj = search.create({
                type: "customer",
                filters:
                [
                   ["externalid","anyof",customerExtId]
                ],
                columns:
                [
                   search.createColumn({name: "externalid", label: "External ID"}),
                   search.createColumn({
                      name: "entityid",
                      sort: search.Sort.ASC,
                      label: "ID"
                   }),
                   search.createColumn({name: "altname", label: "Name"}),
                   search.createColumn({
                      name: "formulatext",
                      formula: "'_migrated'",
                      label: "Formula (Text)"
                   }),
                   search.createColumn({name: "internalid", label: "Internal ID"})
                ]
            });
            customerSearchObj.run().each(function(result){
                //log.audit('Found the duplicate customer!', result.getValue({ name: 'internalid'}));
                duplicateCustomer = result.getValue({ name: 'internalid'});
                duplicateCustomerExtId = result.getValue({ name: 'externalid'});
                addedText = result.getValue({ name: 'formulatext'});
                //log.audit('Formula text', addedText);
                return false;
    
                //Future development concerns: What if there's two or more with the same external ID but already have modified headers
                //Is such a thing possible?
            });
    
            //Change the external ID of the old one, and add the formula text.
            duplicateCustomer = record.submitFields({
                type: record.Type.CUSTOMER,
                id: duplicateCustomer,
                values: {
                    externalid: duplicateCustomerExtId + addedText
                },
                options: {
                    enableSourcing: false,
                    ignoreMandatoryFields: true
                }
            });
            log.audit('duplicateCustomer successfully saved!', duplicateCustomer);
        }
        catch(e){
            log.error('Error on MAP', e);
        }
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
