/**
 * @NApiVersion 2.x
 * @NModuleScope SameAccount
 * @NScriptType Restlet
 */
define(["require", "exports", "N/record", "N/log", "N/search"], function (require, exports, record, log, search) {
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.get = function (getData) {
        var returnData = {};
        try {
        }
        catch (ex) {
        }
        return returnData;
    };
    // noinspection JSUnusedGlobalSymbols
    exports.post = function (getData) {
        var returnData = {};
        log.audit('getData', getData);
        returnData.haysEventId = createHaysEvent(getData);
        if (returnData !== null) {
            return returnData;
        }
    };
    var createHaysEvent = function (haysPayload) {
        var haysEvent = record.create({
            type: HaysEvent.recordId
        });
        // haysEvent.setValue({fieldId: HaysEvent.fields.EXTERNALID, value: haysPayload[0].externalid});
        haysEvent.setValue({ fieldId: HaysEvent.fields.PAYLOAD, value: JSON.stringify(haysPayload) });
        haysEvent.setValue({ fieldId: HaysEvent.fields.STATUS, value: 1 });
        switch (haysPayload[0].recordtype) {
            case 'customer':
                haysEvent.setValue({ fieldId: HaysEvent.fields.RECORDTYPE, value: 1 });
                break;
            case 'invoice':
                haysEvent.setValue({ fieldId: HaysEvent.fields.RECORDTYPE, value: 2 });
                break;
            case 'payment':
                haysEvent.setValue({ fieldId: HaysEvent.fields.RECORDTYPE, value: 3 });
                break;
            case 'creditMemo':
                haysEvent.setValue({ fieldId: HaysEvent.fields.RECORDTYPE, value: 4 });
                break;
            case 'refund':
                haysEvent.setValue({ fieldId: HaysEvent.fields.RECORDTYPE, value: 5 });
                break;
            default:
                log.audit('Record Not Yet Supported', haysPayload[0].recordtype);
                break;
        }
        var subsidiaryId = findSubsidiary("" + haysPayload[0].fields.subsidiary.externalId);
        if (+subsidiaryId > 0) {
            haysEvent.setValue({ fieldId: HaysEvent.fields.SUBSIDIARY, value: subsidiaryId });
        }
        return +haysEvent.save();
    };
    var findSubsidiary = function (subsidiaryname) {
        var subsidiaryId = null;
        try {
            search.create({
                type: 'subsidiary',
                filters: [['name', 'contains', subsidiaryname]],
            }).run().each(function (result) {
                subsidiaryId = +result.id;
                return false;
            });
        }
        catch (e) { }
        return subsidiaryId;
    };
    var HaysEvent = /** @class */ (function () {
        function HaysEvent() {
        }
        HaysEvent.recordId = 'customrecord_hays_event';
        HaysEvent.fields = {
            EXTERNALID: 'externalid',
            PAYLOAD: 'custrecord_hays_event_payload',
            STATUS: 'custrecord_hays_event_status',
            RECORDTYPE: 'custrecord_hays_event_recordtype',
            ERROR: 'custrecord_hays_event_error',
            SUBSIDIARY: 'custrecord_hays_event_subsidiary'
        };
        return HaysEvent;
    }());
    exports.HaysEvent = HaysEvent;
});
