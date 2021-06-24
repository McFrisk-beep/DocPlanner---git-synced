/**
 * @NApiVersion 2.0
 * @NModuleScope SameAccount
 * @NScriptType MapReduceScript
 */
 define(["require", "exports", "N/log", "N/search", "N/record", "N/runtime", "N/error", "N/email"], function(require, exports, log, search, record, runtime, nsError, nsEmail) {
    Object.defineProperty(exports, "__esModule", {
      value: true
    });
  
    var SAT_PAYMENT_METHOD = {
      CASH: 1,
      CHECK: 2,
      ELECTRONIC_FUNDS_TRANSFER: 3,
      CREDIT_CARD: 4,
      ELECTRONIC_WALLET: 5,
      ELECTRONIC_MONEY: 6,
      DIGITAL_CARDS: 7,
      MEAL_VOUCHERS: 8,
      GOODS: 9,
      SERVICE: 10,
      THIRD_PARTY_PAYMENT: 11,
      GIVING_IN_PAYMENT: 12,
      PAYMENT_BY_SUBROGATION: 13,
      PAYMENT_BY_CONSIGNMENT: 14,
      CONDONATION: 15,
      CANCELLATION: 16,
      COMPENSATION: 17,
      NOVATION: 18,
      NETTING: 19,
      DEBT_REMITTANCE: 20,
      PRESCRIPTION_OR_EXPIRATION: 21,
      TO_THE_SATISFACTION_OF_THE_CREDITOR: 22,
      DEBIT_CARD: 23,
      SERVICE_CARD: 24,
      ADVANCE_PAYMENT: 25,
      PAYMENT_INTERMEDIARY: 26,
      NA: 27,
      TO_BE_DEFINED: 28
    };
  
    var SUBSIDIARY = {
      DOCPLANNER_HOLDINGS_BV: 1,
      DOCPLANNER_SUBHOLDINGS_BV: 2,
      DOCPLANNER_ITALY_SRL_CON_SOCIO_UNICO: 6,
      DOCPLANNER_TEKNOLOJI_AAZ: 11,
      DOCTORALIA_BRASIL_SERVIASOS_ONLINE_E_SOFTWARE_LTDA: 5,
      DOCTORALIA_INTERNET_SL: 10,
      DOCTORALIA_MEXICO_SA_DE_CV: 8,
      IANIRI_INFORMATICA_SRL: 13,
      TUOTEMPO_SRL: 7,
      ZNANYLEKARZ_SP_Z_O_O: 9
    }
  
    var DEFAULT_KPI_CATEG = 24; // Revenue;
  
    // Journal Entry Default Business Segment
    var JE_DFLT_BUSSEGMENT = ''
  
    // Payment Default Business Segment
    var PYMT_DFLT_BUSSEGMENT = ''
  
    exports.getInputData = function() {
      var savedSearch_id = runtime.getCurrentScript().getParameter({ name: 'custscript_hays_savedsearch' });
      if(isEmpty(savedSearch_id)) {
        var recordTypeId = +runtime.getCurrentScript().getParameter({ name: 'custscript_hays_record_type' });
        var statusId = +(runtime.getCurrentScript().getParameter({ name: 'custscript_hays_status' }) || 1);
        var haysEventId = runtime.getCurrentScript().getParameter({ name: 'custscript_hays_event' });
        var filters = [
          ['custrecord_hays_event_recordtype', 'anyof', recordTypeId]
        ];
        filters.push('AND', ['custrecord_hays_event_status', 'anyof', statusId]);
        if(!isEmpty(haysEventId)) filters.push('AND', ['internalid', 'anyof', haysEventId]);
        return search.create({
          type: 'customrecord_hays_event',
          filters: filters,
          columns: [
            search.createColumn({
              name: 'internalid',
              label: 'Internal ID'
            }),
            search.createColumn({
              name: 'lastmodified',
              sort: search.Sort.DESC,
              label: 'Last Modified'
            })
          ]
        });
      } else {
        return search.load({ id: savedSearch_id })
      }
  
    };
  
    exports.map = function(context) {
      var searchResult = JSON.parse(context.value);
      context.write(searchResult.id, "" + searchResult.recordType); // Should only be one anyway
    };
  
    exports.reduce = function(context) {
      var event = record.load({
        type: 'customrecord_hays_event',
        id: context.key
      });
      var eventSubsidiary = event.getValue('custrecord_hays_event_subsidiary');
      if(eventSubsidiary != SUBSIDIARY.DOCTORALIA_INTERNET_SL && eventSubsidiary != SUBSIDIARY.DOCPLANNER_ITALY_SRL_CON_SOCIO_UNICO && eventSubsidiary != SUBSIDIARY.DOCTORALIA_MEXICO_SA_DE_CV && eventSubsidiary != SUBSIDIARY.DOCPLANNER_TEKNOLOJI_AAZ && eventSubsidiary != SUBSIDIARY.ZNANYLEKARZ_SP_Z_O_O && eventSubsidiary != SUBSIDIARY.DOCTORALIA_BRASIL_SERVIASOS_ONLINE_E_SOFTWARE_LTDA) return;
      var payload = "" + event.getValue({
        fieldId: 'custrecord_hays_event_payload'
      });
      var haysRecords = JSON.parse(payload);
      var errorLogs = [];
      try {
        var script_1 = runtime.getCurrentScript();
        var internalRetryCount = 0;
        for(var i = 0; i < haysRecords.length; i++) {
          var hays = haysRecords[i];
          try {
            findExistingRecord({
              externalid: hays.externalid,
              recordtype: haysRecordTypeToNSType(hays.recordtype),
              exists: function(foundRecord) {
                var netsuiteRecord = record.load({
                  type: foundRecord.type,
                  id: foundRecord.id,
                  isDynamic: true
                });
                switch (hays.recordtype) {
                  case 'customer':
                    createCustomer(netsuiteRecord, hays, event);
                    break;
                  case 'invoice':
                    // createInvoice(netsuiteRecord, hays, event);
                    throw nsError.create({
                      message: 'Invoice with external id "' + hays.externalid + '" already exists.',
                      name: 'INVOICE_ALREADY_EXISTS',
                      notifyOff: true
                    });
                    break;
                  case 'payment':
                    // createPayment(netsuiteRecord, hays, event);
                    throw nsError.create({
                      message: 'Payment with external id "' + hays.externalid + '" already exists.',
                      name: 'PAYMENT_ALREADY_EXISTS',
                      notifyOff: true
                    });
                    break;
                  case 'creditMemo':
                    if (hays.isPayment === true) {
                      createPayment(netsuiteRecord, hays, event);
                    } else {
                      createCreditMemo(netsuiteRecord, hays, event);
                    }
                    break;
                  case 'refund':
                    // createCustomerRefund(netsuiteRecord, hays, event);
                    throw nsError.create({
                      message: 'Customer Refund with external id "' + hays.externalid + '" already exists.',
                      name: 'REFUND_ALREADY_EXISTS',
                      notifyOff: true
                    });
                    break;
                  default:
                    break;
                }
              },
              doesnotexist: function() {
                var netsuiteRecord = record.create({
                  type: haysRecordTypeToNSType(hays.recordtype),
                  isDynamic: true
                });
                netsuiteRecord.setValue({
                  fieldId: 'externalid',
                  value: hays.externalid
                });
                switch (hays.recordtype) {
                  case 'customer':
                    createCustomer(netsuiteRecord, hays, event);
                    break;
                  case 'invoice':
                    createInvoice(netsuiteRecord, hays, event);
                    break;
                  case 'payment':
                    var isCancellation = hays.fields.cancellation;
                    var referenceInvoiceExtId = hays.fields.reference.externalid;
                    if(isCancellation) createCreditMemo(netsuiteRecord, hays, event, referenceInvoiceExtId);
                    else createPayment(netsuiteRecord, hays, event);
                    break;
                  case 'creditMemo':
                    if (hays.isPayment === true) {
                      createPayment(netsuiteRecord, hays, event);
                    } else {
                      createCreditMemo(netsuiteRecord, hays, event);
                    }
                    break;
                  case 'refund':
                    var refund = hays.fields;
                    if (refund.isChargeback) {
                      createJournalEntry(netsuiteRecord, hays, event);
                    } else {
                      createCustomerRefund(netsuiteRecord, hays, event);
                    }
                    break;
                  default:
                    break;
                }
              }
            });
          } catch (err) {
            if(hays.recordtype == 'customer' && internalRetryCount < 4 && (err.name == 'UNIQUE_CUST_ID_REQD' || err.name == 'DUP_ENTITY' || err.name == 'RCRD_HAS_BEEN_CHANGED')) {
              i--;
              internalRetryCount++;
              continue;
            }
  
            errorLogs.push({
              externalid: hays.externalid,
              recordtype: hays.recordtype,
              error: JSON.stringify(err)
            });
          }
          if (script_1.getRemainingUsage() <= 200) {
            errorLogs.push({
              externalid: hays.externalid,
              recordtype: hays.recordtype,
              error: "PAYLOAD TOO LONG"
            });
            throw nsError.create({
              message: 'PAYLOAD TOO LONG',
              name: 'PAYLOAD_TOO_LONG',
              notifyOff: false
            });
          }
        }
      } catch (er) {
        log.error('ERROR', er);
      }
      try {
        if (errorLogs.length === 0) {
          updateHayEventStatus(+context.key, STATUS.SUCCESS, '');
        } else {
          updateHayEventStatus(+context.key, STATUS.FAILED, JSON.stringify(errorLogs), errorLogs);
        }
      } catch (e) {
        log.error('---- ERROR ----', e);
      }
      context.write(context.key, "customrecord_hays_event"); // Should only be one anyway
    };
  
    exports.summarize = function(context) {
      if (context.inputSummary.error)
        log.error("inputSummary}", context.inputSummary.error);
      context.mapSummary.errors.iterator().each(function(key, error, executionNo) {
        log.error("Map error for Key: " + key + ", Execution No:  " + executionNo, error);
        return true;
      });
      //
      context.reduceSummary.errors.iterator().each(function(key, error, executionNo) {
        //  updateHayEventStatus(+key, STATUS.FAILED, error);
        log.error("Reduce error for Key: " + key + ", Execution No:  " + executionNo, error);
        return true;
      });
      context.output.iterator().each((function(key, value) {
        log.audit('summarize', key + " : " + value);
        return true;
      }));
  
      /* nsEmail.send({
        author: 3,
        body: 'HAYS Map/Reduce Script Finished Execution',
        recipients: 3,
        subject: 'HAYS Map/Reduce Script Finished Execution'
      }); */
    };
  
  
    var convertHaysDateToNSDate = function(haysDate) {
      var jsDate = new Date();
      try {
        if (haysDate.date) {
          var dateArray = ("" + haysDate.date.split(' ')[0]).split('-');
          jsDate = new Date(+dateArray[0], +dateArray[1] - 1, +dateArray[2]);
        }
      } catch (e) {}
      return jsDate;
    };
    var convertHaysCurrencyToNSCurrency = function(haysCurrency) {
      var nsCurrency = null;
      switch (haysCurrency) {
        case 'EUR':
          nsCurrency = 1;
          break;
        case 'USD':
          nsCurrency = 2;
          break;
        case 'CAD':
          nsCurrency = 3;
          break;
        case 'GBP':
          nsCurrency = 4;
          break;
        case 'BRL':
          nsCurrency = 5;
          break;
        case 'MXN':
          nsCurrency = 6;
          break;
        case 'TRY':
          nsCurrency = 7;
          break;
        case 'PLN':
          nsCurrency = 8;
          break;
        case 'ARS':
          nsCurrency = 9;
          break;
        case 'COP':
          nsCurrency = 10;
          break;
        case 'CLP':
          nsCurrency = 11;
          break;
        case 'CZK':
          nsCurrency = 12;
          break;
        case 'CHF':
          nsCurrency = 15;
          break;
        case 'PEN':
          nsCurrency = 13;
          break;
        case 'HUF':
          nsCurrency = 14;
          break;
        case 'AED':
          nsCurrency = 16;
          break;
        default:
          break;
      }
      return nsCurrency;
    };
    var convertHaysStateToNSState = function(haysState, subsidiaryId) {
      var nsState = null;
      if (+subsidiaryId === SUBSIDIARIES.MEXICO) {
        switch (haysState) {
          case 'Yucatan':
            haysState = 'Yucatán';
            break;
          case 'Mexico':
            haysState = 'México';
            break;
          case 'Michoacan':
            haysState = 'Michoacán';
            break;
          case 'Nuevo Leon':
            haysState = 'Nuevo León';
            break;
          case 'Queretaro':
            haysState = 'Querétaro';
            break;
          case 'San Luis Potosi':
            haysState = 'San Luis Potosí';
            break;
          case 'Distrito Federal DF':
            haysState = 'Distrito Federal';
            break;
          case 'null':
          case null:
          case '':
          case undefined:
            haysState = 'Mexico City';
            break;
          default:
            break;
        }
        try {
          search.create({
            type: 'state',
            filters: [
              ['fullname', 'contains', haysState.trim()],
              'AND',
              ['country', 'anyof', 'MX']
            ]
          }).run().each(function(result) {
            if (+result.id > 0) {
              nsState = +result.id;
            }
            return true;
          });
        } catch (e) {
          log.error('error', e);
        }
      }
      if (+nsState <= 0) {
        nsState = 533; // Default to Mexico
      }
      return nsState;
    };
    var convertHaysChannelToNSChannel = function(haysChannel) {
      var acquisitionChannel = null;
      switch (haysChannel) {
        case 'ecommerce_telesale':
          acquisitionChannel = 4;
          break;
        default:
          acquisitionChannel = 5;
          break;
      }
      return acquisitionChannel;
    };
    var setItemSublist = function(netsuiteRecord, itemListField) {
      itemListField.forEach(function(itemList) {
        var script_1 = runtime.getCurrentScript();
        if (script_1.getRemainingUsage() <= 100) {
          throw nsError.create({
            message: 'PAYLOAD TOO LONG',
            name: 'PAYLOAD_TOO_LONG',
            notifyOff: false
          });
        }
        findExistingRecord({
          externalid: itemList.fields.item.externalId,
          recordtype: 'item',
          filters: [
            ['nameinternal', 'is', itemList.fields.item.externalId]
          ],
          exists: function(item) {
            var taxCodeId = setTaxCode(netsuiteRecord, +itemList.fields.taxrate1);
            netsuiteRecord.selectNewLine({
              sublistId: ItemSublist.sublistId
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.ITEM,
              value: item.id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.QUANTITY,
              value: +itemList.fields.quantity
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.DESCRIPTION,
              value: itemList.fields.description
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.RATE,
              value: Number(itemList.fields.amount)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: Number(itemList.fields.amount) * Number(itemList.fields.quantity)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.CLASS,
              value: item.getValue('class')
            });
            var mainLineClass = netsuiteRecord.getValue('class');
            if(isEmpty(mainLineClass)) netsuiteRecord.setValue('class', item.getValue('class'));
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.LINEID,
              value: itemList.fields.line_id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.TAXCODE,
              value: taxCodeId
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.REVRECSTARTDATE,
              value: convertHaysDateToNSDate(itemList.fields.start_date)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.REVRECENDDATE,
              value: convertHaysDateToNSDate(itemList.fields.end_date)
            });
  
            var lineDepartment = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department'
            });
            if(isEmpty(lineDepartment)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department',
              value: netsuiteRecord.getValue('department')
            });
  
            var lineMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets'
            });
            if(isEmpty(lineMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets',
              value: netsuiteRecord.getValue('cseg_markets')
            });
  
            var lineKPICategory = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category'
            });
            if(isEmpty(lineKPICategory)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category',
              value: netsuiteRecord.getValue('cseg_kpi_category')
            });
  
            var lineKPIMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market'
            });
            if(isEmpty(lineKPIMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market',
              value: netsuiteRecord.getValue('cseg_kpi_market')
            });
  
            var lineLocation = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location'
            });
            if(isEmpty(lineLocation)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location',
              value: netsuiteRecord.getValue('location')
            });
  
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
          },
          doesnotexist: function() {
            log.error('Item Not Found', "" + itemList.fields.item.externalId);
            throw nsError.create({
              message: 'One of the Items is not yet existing in NetSuite. Please contact your Global Administrator.',
              name: 'ITEM_NOT_FOUND',
              notifyOff: false
            });
          },
          internalIdOnly: false
        });
      });
    };
    var setCreditMemoSublist = function(netsuiteRecord, ItemSublistArray, invoiceId) {
      var totalPayment = 0;
      var modified = false;
      ItemSublistArray.forEach(function(itemList) {
        var index = netsuiteRecord.findSublistLineWithValue({
          sublistId: ItemSublist.sublistId,
          fieldId: ItemSublist.fields.LINEID,
          value: itemList.fields.base_line_id
        });
        if (index !== -1) {
          netsuiteRecord.selectLine({
            sublistId: ItemSublist.sublistId,
            line: index
          });
          var currentAmount = +netsuiteRecord.getCurrentSublistValue({
            sublistId: ItemSublist.sublistId,
            fieldId: ItemSublist.fields.AMOUNT
          });
          var newAmount = +itemList.fields.amount;
          var difference = currentAmount - newAmount;
          totalPayment += difference;
  
          if (newAmount === 0) {
            // do nothing // fully credited
          } else {
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.LINEID,
              value: itemList.fields.base_line_id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: difference
            });
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
            modified = true;
          }
        } else {
          // log.error('--- base_line_id not found', itemList.fields.base_line_id);
        }
      });
      var applyIndex = netsuiteRecord.findSublistLineWithValue({
        sublistId: 'apply ',
        fieldId: 'internalid',
        value: invoiceId
      });
      if (modified) {
        // Remove all Lines
        var i = +netsuiteRecord.getLineCount({
          sublistId: 'apply'
        }) - 1;
        if (i !== -1) {
          while (i >= 0) {
            netsuiteRecord.selectLine({
              sublistId: 'apply',
              line: i
            });
            var id = netsuiteRecord.getCurrentSublistValue({
              sublistId: 'apply',
              fieldId: 'internalid'
            });
            if ("" + id === "" + invoiceId) {
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                value: totalPayment
              });
              netsuiteRecord.commitLine({
                sublistId: 'apply'
              });
            }
            i = i - 1;
          }
        }
      }
      /*
      if (applyIndex > 0 && modified) {
          let origTotalAmount = +netsuiteRecord.getSublistValue({sublistId: ItemSublist.sublistId, fieldId: ItemSublist.fields.AMOUNT,  line: applyIndex});
          netsuiteRecord.setSublistValue({sublistId: ItemSublist.sublistId, fieldId: ItemSublist.fields.AMOUNT, value: origTotalAmount - totalPayment, line: applyIndex});
      } */
    };
  
    var setCreditMemoSublist_mexico = function(netsuiteRecord, ItemSublistArray, invoiceId) {
      var totalPayment = 0;
      var modified = false;
      ItemSublistArray.forEach(function(itemList) {
        var index = netsuiteRecord.findSublistLineWithValue({
          sublistId: ItemSublist.sublistId,
          fieldId: ItemSublist.fields.LINEID,
          value: itemList.fields.base_line_id
        });
        if (index !== -1) {
          netsuiteRecord.selectLine({
            sublistId: ItemSublist.sublistId,
            line: index
          });
          var currentAmount = +netsuiteRecord.getCurrentSublistValue({
            sublistId: ItemSublist.sublistId,
            fieldId: ItemSublist.fields.AMOUNT
          });
          var newAmount = +itemList.fields.amount;
          var difference = currentAmount - newAmount;
  
          //Added tax due to issues in unapplied Credit Memos due to lacking taxes (added by: Raph)
          //If you're looking at this, and there's issues here. Good luck buddy.
          var taxAmount = netsuiteRecord.getCurrentSublistValue({
            sublistId: ItemSublist.sublistId,
            fieldId: 'tax1amt'
          });
  
          if(difference > 0){
            totalPayment += (difference + taxAmount);
          }
          else{
            totalPayment += difference;
          }
          //End changes
          //log.error('CHECKER 1-4 totalPayment', totalPayment + ' | ' + currentAmount + ' | ' + newAmount + ' | ' + taxAmount);
  
          if (newAmount === 0) {
            // do nothing // fully credited
          } else {
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.LINEID,
              value: itemList.fields.base_line_id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: difference
            });
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
            modified = true;
          }
        } else {
          // log.error('--- base_line_id not found', itemList.fields.base_line_id);
        }
      });
      var applyIndex = netsuiteRecord.findSublistLineWithValue({
        sublistId: 'apply ',
        fieldId: 'internalid',
        value: invoiceId
      });
      if (modified) {
        // Remove all Lines
        var i = +netsuiteRecord.getLineCount({
          sublistId: 'apply'
        }) - 1;
        if (i !== -1) {
          while (i >= 0) {
            netsuiteRecord.selectLine({
              sublistId: 'apply',
              line: i
            });
            var id = netsuiteRecord.getCurrentSublistValue({
              sublistId: 'apply',
              fieldId: 'internalid'
            });
            if ("" + id === "" + invoiceId) {
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                value: totalPayment
              });
              netsuiteRecord.commitLine({
                sublistId: 'apply'
              });
            }
            i = i - 1;
          }
        }
      }
      /*
      if (applyIndex > 0 && modified) {
          let origTotalAmount = +netsuiteRecord.getSublistValue({sublistId: ItemSublist.sublistId, fieldId: ItemSublist.fields.AMOUNT,  line: applyIndex});
          netsuiteRecord.setSublistValue({sublistId: ItemSublist.sublistId, fieldId: ItemSublist.fields.AMOUNT, value: origTotalAmount - totalPayment, line: applyIndex});
      } */
    };
  
    var setCreditMemoSublist_brazil = function(netsuiteRecord, ItemSublistArray, invoiceId) {
      var totalPayment = 0;
      var modified = false;
      ItemSublistArray.forEach(function(itemList) {
        var index = netsuiteRecord.findSublistLineWithValue({
          sublistId: ItemSublist.sublistId,
          fieldId: ItemSublist.fields.LINEID,
          value: itemList.fields.base_line_id
        });
        if (index !== -1) {
          netsuiteRecord.selectLine({
            sublistId: ItemSublist.sublistId,
            line: index
          });
          var currentAmount = +netsuiteRecord.getCurrentSublistValue({
            sublistId: ItemSublist.sublistId,
            fieldId: ItemSublist.fields.AMOUNT
          });
          var newAmount = +itemList.fields.amount;
          var difference = currentAmount - newAmount;
          totalPayment += difference;
          if (newAmount === 0) {
            // do nothing // fully credited
          } else {
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.LINEID,
              value: itemList.fields.base_line_id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: difference
            });
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
            modified = true;
          }
        } else {
          // log.error('--- base_line_id not found', itemList.fields.base_line_id);
        }
      });
      var applyIndex = netsuiteRecord.findSublistLineWithValue({
        sublistId: 'apply ',
        fieldId: 'internalid',
        value: invoiceId
      });
      if (modified) {
        // Remove all Lines
        var i = +netsuiteRecord.getLineCount({
          sublistId: 'apply'
        }) - 1;
        if (i !== -1) {
          while (i >= 0) {
            netsuiteRecord.selectLine({
              sublistId: 'apply',
              line: i
            });
            var id = netsuiteRecord.getCurrentSublistValue({
              sublistId: 'apply',
              fieldId: 'internalid'
            });
            if ("" + id === "" + invoiceId) {
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
              netsuiteRecord.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                value: totalPayment
              });
              netsuiteRecord.commitLine({
                sublistId: 'apply'
              });
            }
            i = i - 1;
          }
        }
      }
    };
    var setCreditMemoInvoiceSublist = function(netsuiteRecord, ItemSublistArray) {
      var foundLines = [];
      // Remove all Lines
      var i = +netsuiteRecord.getLineCount({
        sublistId: 'item'
      }) - 1;
      if (i !== -1) {
        while (i >= 0) {
          netsuiteRecord.removeLine({
            sublistId: 'item',
            line: i
          });
          i = i - 1;
        }
      }
      ItemSublistArray.forEach(function(itemList) {
        findExistingRecord({
          externalid: itemList.fields.item.externalId,
          recordtype: 'item',
          filters: [
            ['nameinternal', 'is', itemList.fields.item.externalId]
          ],
          exists: function(item) {
            var taxCodeId = setTaxCode(netsuiteRecord, +itemList.fields.taxrate1);
            netsuiteRecord.selectNewLine({
              sublistId: ItemSublist.sublistId
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.ITEM,
              value: item.id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.QUANTITY,
              value: +itemList.fields.quantity
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.DESCRIPTION,
              value: itemList.fields.description
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.RATE,
              value: Number(itemList.fields.amount)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: Number(itemList.fields.amount) * Number(itemList.fields.quantity)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.CLASS,
              value: item.getValue('class')
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.TAXCODE,
              value: taxCodeId
            });
  
            var lineDepartment = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department'
            });
            if(isEmpty(lineDepartment)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department',
              value: netsuiteRecord.getValue('department')
            });
  
            var lineMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets'
            });
            if(isEmpty(lineMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets',
              value: netsuiteRecord.getValue('cseg_markets')
            });
  
            var lineKPICategory = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category'
            });
            if(isEmpty(lineKPICategory)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category',
              value: netsuiteRecord.getValue('cseg_kpi_category')
            });
  
            var lineKPIMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market'
            });
            if(isEmpty(lineKPIMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market',
              value: netsuiteRecord.getValue('cseg_kpi_market')
            });
  
            var lineLocation = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location'
            });
            if(isEmpty(lineLocation)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location',
              value: netsuiteRecord.getValue('location')
            });
  
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
          },
          doesnotexist: function() {
            log.error('Item Not Found', "" + itemList.fields.item.externalId);
          },
          internalIdOnly: false
        });
      });
    };
    var setCreditMemoInvoiceSublist_brazil = function(netsuiteRecord, ItemSublistArray) {
      var foundLines = [];
      // Remove all Lines
      var i = +netsuiteRecord.getLineCount({
        sublistId: 'item'
      }) - 1;
      if (i !== -1) {
        while (i >= 0) {
          netsuiteRecord.removeLine({
            sublistId: 'item',
            line: i
          });
          i = i - 1;
        }
      }
      ItemSublistArray.forEach(function(itemList) {
        findExistingRecord({
          externalid: itemList.fields.item.externalId,
          recordtype: 'item',
          filters: [
            ['nameinternal', 'is', itemList.fields.item.externalId]
          ],
          exists: function(item) {
            var taxCodeId = setTaxCode(netsuiteRecord, +itemList.fields.taxrate1);
            netsuiteRecord.selectNewLine({
              sublistId: ItemSublist.sublistId
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.ITEM,
              value: item.id
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.QUANTITY,
              value: +itemList.fields.quantity
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.DESCRIPTION,
              value: itemList.fields.description
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.RATE,
              value: Number(itemList.fields.amount)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.AMOUNT,
              value: Number(itemList.fields.amount) * Number(itemList.fields.quantity)
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.CLASS,
              value: item.getValue('class')
            });
            netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: ItemSublist.fields.TAXCODE,
              value: taxCodeId
            });
  
            var lineDepartment = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department'
            });
            if(isEmpty(lineDepartment)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'department',
              value: netsuiteRecord.getValue('department')
            });
  
            var lineMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets'
            });
            if(isEmpty(lineMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_markets',
              value: netsuiteRecord.getValue('cseg_markets')
            });
  
            var lineKPICategory = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category'
            });
            if(isEmpty(lineKPICategory)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_category',
              value: netsuiteRecord.getValue('cseg_kpi_category')
            });
  
            var lineKPIMarket = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market'
            });
            if(isEmpty(lineKPIMarket)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'cseg_kpi_market',
              value: netsuiteRecord.getValue('cseg_kpi_market')
            });
  
            var lineLocation = netsuiteRecord.getCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location'
            });
            if(isEmpty(lineLocation)) netsuiteRecord.setCurrentSublistValue({
              sublistId: ItemSublist.sublistId,
              fieldId: 'location',
              value: netsuiteRecord.getValue('location')
            });
  
            netsuiteRecord.commitLine({
              sublistId: ItemSublist.sublistId
            });
          },
          doesnotexist: function() {
            log.error('Item Not Found', "" + itemList.fields.item.externalId);
          },
          internalIdOnly: false
        });
      });
    };
    var setTaxCode = function(netsuiteRecord, taxRate) {
      var subsidiaryId = +netsuiteRecord.getValue('subsidiary');
      var marketId = +netsuiteRecord.getValue('cseg_markets');
      var taxCodeId = 41;
      if (!taxRate) {
        taxRate = 0;
      }
      var taxcodeSearch = search.create({
        type: 'customrecord_hays_taxcode_mapping',
        filters: [
          ['custrecord_hays_taxcode_subsidiary', 'anyof', subsidiaryId],
          'AND',
          ['custrecord_hays_taxcode_country', 'anyof', marketId],
          'AND',
          ['custrecord_hays_taxcode_rate', 'equalto', taxRate]
        ],
        columns: [
          search.createColumn({
            name: 'custrecord_hays_taxcode_taxcode',
            label: 'Tax Code'
          })
        ]
      });
      taxcodeSearch.run().each(function(result) {
        taxCodeId = +result.getValue('custrecord_hays_taxcode_taxcode');
        return false;
      });
      return taxCodeId;
    };
    var createCustomer = function(netsuiteRecord, hays, haysEvent) {
      var customer = hays.fields;
      var subsidiaryId = findSubsidiary("" + customer.subsidiary.externalId);
      if (+subsidiaryId > 0) {
        var formId = getCustomFormId(subsidiaryId, 'customer');
        if (+formId > 0) {
          netsuiteRecord.setValue({
            fieldId: 'customform',
            value: formId
          });
        }
        netsuiteRecord.setValue({
          fieldId: 'subsidiary',
          value: subsidiaryId
        });
      }
      netsuiteRecord.setValue({
        fieldId: 'externalid',
        value: hays.externalid
      });
      netsuiteRecord.setValue({
        fieldId: 'custentity_ats_source',
        value: 2
      });
      netsuiteRecord.setValue({
        fieldId: 'custentity_hays_event',
        value: +haysEvent.id
      });
      netsuiteRecord.setValue({
        fieldId: 'companyname',
        value: customer.companyname.substring(0, 82)
      });
      // New error with company field length exceeding 82
      if (customer.companyname && customer.companyname.length > 82) {
        netsuiteRecord.setValue({
          fieldId: 'memo',
          value: "Company Name Too Long: " + customer.companyname
        });
      }
      switch (subsidiaryId) {
        case SUBSIDIARIES.TURKEY:
          if (customer.tax_number && customer.tax_number.length > 10) {
            netsuiteRecord = setIndividual(netsuiteRecord, customer);
          }
          break;
        case SUBSIDIARIES.BRAZIL:
          if (customer.customer_type) {
            switch (customer.customer_type) {
              case 'CNPJ': // Company
                netsuiteRecord.setValue({
                  fieldId: 'custentity_psg_br_cnpj',
                  value: customer.tax_number
                });
                break;
              default: // individual
                var cnpj_field = netsuiteRecord.getField({ fieldId: 'custentity_psg_br_cnpj' });
                cnpj_field.isMandatory = false;
                netsuiteRecord = setIndividual(netsuiteRecord, customer);
                netsuiteRecord.setValue({
                  fieldId: 'custentity_psg_br_cpf',
                  value: customer.tax_number
                });
                break;
            }
          }
          break;
        case SUBSIDIARIES.MEXICO:
          if (customer.tax_number && customer.tax_number.length === 13) {
            netsuiteRecord = setIndividual(netsuiteRecord, customer);
          } else {
            netsuiteRecord.setValue({
              fieldId: 'isperson',
              value: 'F'
            });
            netsuiteRecord.setValue({
              fieldId: 'companyname',
              value: customer.companyname.substring(0, 82)
            });
          }
          netsuiteRecord.setValue({
            fieldId: 'custentity_mx_rfc',
            value: customer.tax_number
          });
          break;
        case SUBSIDIARIES.SPAIN:
          var paisesId = getPaisesId(customer.addressbookList.fields.country);
          netsuiteRecord.setValue('custentity_x_sii_xpais', paisesId);
          break;
        default:
          if (customer.type && customer.type === 'Individual') {
            netsuiteRecord = setIndividual(netsuiteRecord, customer);
          }
          break;
      }
      try {
        netsuiteRecord.setValue({
          fieldId: 'email',
          value: customer.email
        });
      } catch(e) {
        netsuiteRecord.setValue({
          fieldId: 'email',
          value: ''
        });
      }
      netsuiteRecord.setValue({
        fieldId: 'url',
        value: customer.url.replace('local.', '')
      });
      netsuiteRecord.setValue({
        fieldId: 'custentity_ats_hayslink',
        value: customer.url.replace('local.', '')
      });
      try {
        netsuiteRecord.setValue({
          fieldId: 'phone',
          value: customer.phone
        });
      } catch(e) {
        netsuiteRecord.setValue({
          fieldId: 'phone',
          value: ''
        });
      }
      if (customer.accountNumber) {
        netsuiteRecord.setValue({
          fieldId: 'accountnumber',
          value: customer.accountNumber
        });
      }
      if (customer.tax_number) {
        netsuiteRecord.setValue({
          fieldId: 'vatregnumber',
          value: customer.tax_number
        });
      }
      if (customer.entity_customer_care_rep) {
        netsuiteRecord.setValue({
          fieldId: 'custentity_ats_customer_care',
          value: "" + customer.entity_customer_care_rep
        });
      }
      if (customer.tax_number && subsidiaryId === SUBSIDIARIES.MEXICO) {
        netsuiteRecord.setValue({
          fieldId: 'custentity_ats_pl_nip',
          value: customer.tax_number
        });
      }
      switch (customer.entityStatus.externalId) {
        case 'not_active':
          netsuiteRecord.setValue({
            fieldId: 'entitystatus',
            value: 18
          });
          break;
        default:
          netsuiteRecord.setValue({
            fieldId: 'entitystatus',
            value: 17
          });
          break;
      }
      if (customer.salesRep && customer.salesRep.externalId) {
        // netsuiteRecord.setValue({fieldId: 'comments', value: `Sales Rep: ${customer.salesRep.externalId}`});
        netsuiteRecord.setValue({
          fieldId: 'salesrep',
          value: findSalesRep("" + customer.salesRep.externalId)
        });
      }
      if (customer.tax_office) {
        netsuiteRecord.setValue({
          fieldId: 'comments',
          value: "Tax Office: " + customer.tax_office
        });
      }
      if (customer.addressbookList && customer.addressbookList.fields) {
        netsuiteRecord.selectNewLine({
          sublistId: AddressSublist.sublistId
        });
        netsuiteRecord.setCurrentSublistValue({
          sublistId: AddressSublist.sublistId,
          fieldId: AddressSublist.fields.DEFAULTBILLING,
          value: true
        });
        netsuiteRecord.setCurrentSublistValue({
          sublistId: AddressSublist.sublistId,
          fieldId: AddressSublist.fields.DEFAULTSHIPPING,
          value: true
        });
        netsuiteRecord.setCurrentSublistValue({
          sublistId: AddressSublist.sublistId,
          fieldId: AddressSublist.fields.LABEL,
          value: "Hays Address"
        });
        var addressSubrecord = netsuiteRecord.getCurrentSublistSubrecord({
          sublistId: AddressSublist.sublistId,
          fieldId: AddressSublist.fields.ADDRESSBOOK
        });
        // If country is czechia, swap it out to czech republic
        if (customer.addressbookList.fields.country === 'Czechia') {
          customer.addressbookList.fields.country = 'Czech Republic';
        }
        if (customer.addressbookList.fields.country) {
          addressSubrecord.setText({
            fieldId: 'country',
            text: "" + customer.addressbookList.fields.country
          });
          try {
            // Set the Market
            netsuiteRecord.setText({
              fieldId: 'custentity_hays_market',
              text: customer.addressbookList.fields.country
            });
          } catch (e) {}
        }
        if (customer.addressbookList.fields.fields.addr1) {
          addressSubrecord.setValue({
            fieldId: 'addr1',
            value: "" + customer.addressbookList.fields.fields.addr1
          });
        }
        if (customer.addressbookList.fields.fields.addr2) {
          addressSubrecord.setValue({
            fieldId: 'addr2',
            value: "" + customer.addressbookList.fields.fields.addr2
          });
        }
        // MEXICO or SPAIN
        if (subsidiaryId === SUBSIDIARIES.MEXICO || subsidiaryId === SUBSIDIARIES.SPAIN) {
          addressSubrecord.setValue({
            fieldId: 'custrecord_streetname',
            value: customer.addressbookList.fields.fields.addr1 || 'Street'
          });
          addressSubrecord.setValue({
            fieldId: 'custrecord_streetnum',
            value: customer.addressbookList.fields.fields.addr2 || 'Street'
          });
          addressSubrecord.setValue({
            fieldId: 'state',
            value: convertHaysStateToNSState(customer.addressbookList.fields.fields.state, subsidiaryId)
          });
          addressSubrecord.setValue({
            fieldId: 'custrecord_province',
            value: customer.addressbookList.fields.fields.state
          });
        }
        else if(subsidiaryId === SUBSIDIARIES.BRAZIL) {
          //BRAZIL
          addressSubrecord.setValue({
              fieldId: 'custrecord_streetname',
              value: customer.addressbookList.fields.fields.addr1 || 'Street'
            });
            addressSubrecord.setValue({
              fieldId: 'custrecord_streetnum',
              value: customer.addressbookList.fields.fields.addr2 || ''
            });
            addressSubrecord.setValue({
              fieldId: 'state',
              value: convertHaysStateToNSState(customer.addressbookList.fields.fields.state, subsidiaryId)
            });
            addressSubrecord.setValue({
              fieldId: 'custrecord_province',
              value: customer.addressbookList.fields.fields.state
            });
        } 
        else {
          if (customer.addressbookList.fields.fields.state) {
            addressSubrecord.setValue({
              fieldId: 'state',
              value: "" + customer.addressbookList.fields.fields.state
            });
          }
        }
        if (customer.addressbookList.fields.fields.addrPhone) {
          try {
            addressSubrecord.setValue({
              fieldId: 'addrphone',
              value: "" + customer.addressbookList.fields.fields.addrPhone
            });
          } catch(e) {
            addressSubrecord.setValue({
              fieldId: 'addrphone',
              value: ''
            });
          }
        }
        if (customer.addressbookList.fields.fields.city) {
          addressSubrecord.setValue({
            fieldId: 'city',
            value: "" + customer.addressbookList.fields.fields.city
          });
        }
        if (customer.addressbookList.fields.fields.zip) {
          addressSubrecord.setValue({
            fieldId: 'zip',
            value: "" + customer.addressbookList.fields.fields.zip
          });
        }
        netsuiteRecord.commitLine({
          sublistId: AddressSublist.sublistId
        });
      }
      var currencyId = convertHaysCurrencyToNSCurrency(customer.currency.externalId);
      if (+currencyId > 0) {
        netsuiteRecord.setValue({
          fieldId: 'currency',
          value: currencyId
        });
      }
      // netsuiteRecord.setText({fieldId: 'custentity_emea_company_reg_num', text: `${customer.accountNumber}`});
      netsuiteRecord.save({ ignoreMandatoryFields: true });
      /* try {
        netsuiteRecord.save({ ignoreMandatoryFields: true });
      } catch(e) {
        if(e.name == 'INVALID_FLD_VALUE') {
          if(isInArray('field: email', e.message)) {
            log.debug('EMAIL e.message', e.message);
            netsuiteRecord.setValue('email', '');
            netsuiteRecord.save({ ignoreMandatoryFields: true });
          } else if(isInArray('Phone number', e.message)) {
            log.debug('PHONE e.message', e.message);
            netsuiteRecord.setValue('phone', '');
            netsuiteRecord.save({ ignoreMandatoryFields: true });
          } else {
            throw e;
          }
        } else {
          throw e
        }
      } */
      // updateHayEventStatus(haysEvent, STATUS.SUCCESS);
    };
    var setIndividual = function(netsuiteRecord, customer) {
      netsuiteRecord.setValue({
        fieldId: 'isperson',
        value: 'T'
      });
      var index = customer.companyname.lastIndexOf(' '); // last occurence of space
      var firstName = customer.name;
      var lastName = customer.surname;
      // New issue with length of field limited to 32
      /* if (!customer.surname) {
        if(index == -1) {
          firstName = customer.companyname;
        } else {
          firstName = customer.companyname.substring(0, index).substring(0, 32);
          lastName = customer.companyname.substring(index + 1).substring(0, 32);
        }
      }*/
      // If its still blank, set to null
      /* if (lastName.length <= 0) {
        lastName = 'null';
      }*/
      if(isEmpty(lastName)) {
        lastName = 'Null';
      }
      if(firstName.length > 32) firstName = firstName.substring(0, 32);
      if(lastName.length > 32) lastName = lastName.substring(0, 32);
  
      netsuiteRecord.setValue({
        fieldId: 'firstname',
        value: firstName
      });
      netsuiteRecord.setValue({
        fieldId: 'lastname',
        value: lastName
      });
      return netsuiteRecord;
    };
    var setIndividual_mexico = function(netsuiteRecord, customer) {
      if(customer.type == 'Company') {
        netsuiteRecord.setValue({
          fieldId: 'isperson',
          value: 'F'
        });
      } else {
        netsuiteRecord.setValue({
          fieldId: 'isperson',
          value: 'T'
        });
      }
      var index = customer.companyname.lastIndexOf(' '); // last occurence of space
      var firstName = customer.name;
      var lastName = customer.surname;
      // New issue with length of field limited to 32
      if (!customer.surname) {
        firstName = customer.companyname.substring(0, index).substring(0, 32);
        lastName = customer.companyname.substring(index + 1).substring(0, 32);
      }
      // If its still blank, set to null
      if (lastName.length <= 0) {
        lastName = '.';
      }
      netsuiteRecord.setValue({
        fieldId: 'firstname',
        value: firstName
      });
      if(!isEmpty(customer.surname)) {
        netsuiteRecord.setValue({
          fieldId: 'lastname',
          value: lastName
        });
      }
      return netsuiteRecord;
    };
    var createInvoice = function(netsuiteRecord, hays, haysEvent) {
      var invoice = hays.fields;
      findExistingRecord({
        externalid: invoice.entity.externalId,
        recordtype: 'customer',
        exists: function(entity) {
          netsuiteRecord.setValue({
            fieldId: 'entity',
            value: entity.id
          });
          var subsidiaryId = +netsuiteRecord.getValue({
            fieldId: 'subsidiary'
          });
          if (+subsidiaryId > 0) {
            var formId = getCustomFormId(subsidiaryId, 'invoice');
            if (+formId > 0) {
              netsuiteRecord.setValue({
                fieldId: 'customform',
                value: formId
              });
            }
            netsuiteRecord.setValue({
              fieldId: 'subsidiary',
              value: subsidiaryId
            });
            netsuiteRecord.setValue({
              fieldId: 'entity',
              value: entity.id
            });
          }
          netsuiteRecord.setValue({
            fieldId: 'externalid',
            value: hays.externalid
          });
          netsuiteRecord.setValue({
            fieldId: 'custbody_hays_event',
            value: +haysEvent.id
          });
          netsuiteRecord.setValue({
            fieldId: 'custbody_ats_source',
            value: 2
          });
          // netsuiteRecord.setValue({fieldId: 'tranid', value:  `${invoice.invoice_number}`.substring(`${invoice.invoice_number}`.lastIndexOf('/') + 1)});
          netsuiteRecord.setValue({
            fieldId: 'tranid',
            value: "" + invoice.invoice_number
          });
          netsuiteRecord.setValue({
            fieldId: 'otherrefnum',
            value: "" + invoice.tranid
          });
          netsuiteRecord.setValue({
            fieldId: 'trandate',
            value: convertHaysDateToNSDate(invoice.trandate)
          });
          netsuiteRecord.setValue({
            fieldId: 'startdate',
            value: convertHaysDateToNSDate(invoice.startDate)
          });
          netsuiteRecord.setValue({
            fieldId: 'enddate',
            value: convertHaysDateToNSDate(invoice.endDate)
          });
          if(subsidiaryId == SUBSIDIARY.ZNANYLEKARZ_SP_Z_O_O) {
            netsuiteRecord.setValue({
              fieldId: 'custbody_ats_pl_vat_date',
              value: convertHaysDateToNSDate(invoice.endDate)
            });
          } else if(subsidiaryId == SUBSIDIARIES.SPAIN){
            netsuiteRecord.setValue('memo', 'Prestación de Servicios');
          }
          var currencyId = convertHaysCurrencyToNSCurrency(invoice.currency);
          if (+currencyId > 0) {
            netsuiteRecord.setValue({
              fieldId: 'currency',
              value: convertHaysCurrencyToNSCurrency(invoice.currency)
            });
          }
          netsuiteRecord.setValue({
            fieldId: 'duedate',
            value: convertHaysDateToNSDate(invoice.dueDate)
          });
          netsuiteRecord.setValue({
            fieldId: 'department',
            value: 2
          });
          netsuiteRecord.setValue({
            fieldId: 'location',
            value: DEFAULT_KPI_CATEG
          });
          netsuiteRecord.setValue({
            fieldId: 'cseg_acq_channels',
            value: convertHaysChannelToNSChannel(invoice.custobody_acquisition_channel)
          });
          if (invoice.uuid) {
            netsuiteRecord.setValue({
              fieldId: 'custbody_mx_cfdi_uuid',
              value: invoice.uuid
            });
          }
          if (invoice.terms) {
            switch (invoice.terms) {
              case 'PUE':
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_term',
                  value: 3
                });
                break;
              case 'PPD':
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_term',
                  value: 4
                });
                break;
              default:
                break;
            }
          }
          if (invoice.invoice_payment_method) {
            switch (+invoice.invoice_payment_method) {
              case 1: // electronic wallet
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_WALLET
                });
                break;
              case 2: // electronic fund transfer
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_FUNDS_TRANSFER
                });
                break;
              case 3: // cash
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CASH
                });
                break;
              case 4: // check
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CHECK
                });
                break;
              case 5: // credit card
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CREDIT_CARD
                });
                break;
              case 6: // electronic money
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_MONEY
                });
                break;
              case 9: // debit card
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.DEBIT_CARD
                });
                break;
              case 99: // to be defined
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.TO_BE_DEFINED
                });
                break;
              default:
                break;
            }
          }
          findExistingRecord({
            recordtype: 'customrecord_hays_location_mapping',
            filters: [
              ['custrecord_hays_location_subsidiary', 'anyof', subsidiaryId], 'AND', ['isinactive', 'is', 'F']
            ],
            exists: function(foundRecord) {
              var locationId = +foundRecord.getValue({
                fieldId: 'custrecord_hays_location_location'
              });
              var locationText = ("" + foundRecord.getText({
                fieldId: 'custrecord_hays_location_location'
              })).split('-')[0].trim();
              // Set the location
              netsuiteRecord.setValue({
                fieldId: 'cseg_kpi_category',
                value: +locationId
              });
              var country = entity.getValue('billcountry');
              switch (country) {
                case 'BR':
                  locationText = 'Brazil';
                  break;
                case 'CO':
                  locationText = 'Colombia';
                  break;
                case 'CZ':
                  locationText = 'Czech';
                  break;
                case 'PE':
                  locationText = 'Peru';
                  break;
                case 'AR':
                  locationText = 'Argentina';
                  break;
                case 'CL':
                  locationText = 'Chile';
                  break;
                case 'PT':
                  locationText = 'Portugal';
                  break;
                default:
                  break;
              }
              // Set the Market
              var entityMarketId = entity.getValue('custentity_hays_market');
              var hasEntityMarketId = false;
              if (+entityMarketId > 0) {
                netsuiteRecord.setValue({
                  fieldId: 'cseg_markets',
                  value: entityMarketId
                });
                netsuiteRecord.setValue({
                  fieldId: 'cseg_kpi_market',
                  value: getKPIMarket(entityMarketId)
                });
                hasEntityMarketId = true;
              } else {
                netsuiteRecord.setText({
                  fieldId: 'cseg_markets',
                  text: locationText
                });
              }
              if(!hasEntityMarketId) {
                findExistingRecord({
                  recordtype: 'customrecord_cseg_kpi_market',
                  filters: [
                    ['cseg_kpi_market_filterby_cseg_markets.name', 'contains', locationText], 'AND', ['isinactive', 'is', 'F']
                  ],
                  exists: function(kpiRecord) {
                    // Set the KPI Market
                    netsuiteRecord.setValue({
                      fieldId: 'cseg_kpi_market',
                      value: +kpiRecord
                    });
                  },
                  doesnotexist: function() {},
                  internalIdOnly: true
                });
              }
            },
            doesnotexist: function() {},
            internalIdOnly: false
          });
          setItemSublist(netsuiteRecord, invoice.itemList.fields);
          var arMapping = findARAccountId(subsidiaryId, +netsuiteRecord.getValue('cseg_markets'));
          var arAccountId = +arMapping['araccount'];
          var arPrefix = arMapping['prefix'];
          if (+arAccountId > 0) {
            netsuiteRecord.setValue({
              fieldId: 'account',
              value: arAccountId
            });
          }
          if (arPrefix && arPrefix.length > 0) {
            // netsuiteRecord.setValue({fieldId: 'tranid', value:  `${arPrefix}${invoice.invoice_number.substring(`${invoice.invoice_number}`.lastIndexOf('/') + 1)}`});
          }
          netsuiteRecord.save();
          // updateHayEventStatus(haysEvent, STATUS.SUCCESS);
        },
        doesnotexist: function() {
          log.error('Entity Not Found', "Externalid - " + invoice.entity.externalId);
          throw nsError.create({
            message: "Entity Not Found, Externalid - " + invoice.entity.externalId,
            name: 'ENTITY_NOT_FOUND',
            notifyOff: false
          });
        },
        internalIdOnly: false
      });
    };
    var createInvoiceFromRefund = function(invoiceRecord, hays, haysEvent) {
      var invoiceId = null;
      try {
        var invoice = hays.fields;
        var subsidiaryId = +invoiceRecord.getValue({
          fieldId: 'subsidiary'
        });
        var formId = getCustomFormId(subsidiaryId, 'invoice');
        var netsuiteRecord = record.copy({
          type: record.Type.INVOICE,
          id: invoiceRecord.id,
          isDynamic: true,
          defaultValue: {
            customform: formId
          }
        });
        // let tranidText = `${invoiceRecord.getValue('tranid')}`;
        // let firstPart = tranidText.substring(0, tranidText.lastIndexOf('/'));
        // let lastPart = tranidText.substring(tranidText.lastIndexOf('/') + 1);
        // const creditMemoTranId = `${firstPart}${getNextTransactionId(lastPart)}`;
        netsuiteRecord.setValue({
          fieldId: 'tranid',
          value: "" + invoice.correction_number
        });
        netsuiteRecord.setValue({
          fieldId: 'externalid',
          value: "" + hays.externalid
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_hays_event',
          value: +haysEvent.id
        });
        netsuiteRecord.setValue({
          fieldId: 'otherrefnum',
          value: "" + invoice.tranid
        });
        netsuiteRecord.setValue({
          fieldId: 'trandate',
          value: convertHaysDateToNSDate(invoice.trandate)
        });
        netsuiteRecord.setValue({
          fieldId: 'currency',
          value: convertHaysCurrencyToNSCurrency(invoice.currency)
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_ats_invoice_from_refund',
          value: true
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_ats_source',
          value: 2
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_refno_originvoice',
          value: invoice.correction_number
        });
        setCreditMemoInvoiceSublist(netsuiteRecord, invoice.itemList.fields);
        invoiceId = netsuiteRecord.save();
      } catch (e) {}
      return invoiceId;
      // updateHayEventStatus(haysEvent, STATUS.SUCCESS);
    };
    var createInvoiceFromRefund_brazil = function(invoiceRecord, hays, haysEvent, creditMemoId) {
      var invoiceId = null;
      try {
        var invoice = hays.fields;
        var subsidiaryId = +invoiceRecord.getValue({
          fieldId: 'subsidiary'
        });
        var formId = getCustomFormId(subsidiaryId, 'invoice');
        var netsuiteRecord = record.copy({
          type: record.Type.INVOICE,
          id: invoiceRecord.id,
          isDynamic: true,
          defaultValue: {
            customform: formId
          }
        });
        netsuiteRecord.setValue({
          fieldId: 'tranid',
          value: "" + invoice.correction_number
        });
        netsuiteRecord.setValue({
          fieldId: 'externalid',
          value: "" + hays.externalid
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_hays_event',
          value: +haysEvent.id
        });
        netsuiteRecord.setValue({
          fieldId: 'otherrefnum',
          value: "" + invoice.tranid
        });
        netsuiteRecord.setValue({
          fieldId: 'trandate',
          value: convertHaysDateToNSDate(invoice.trandate)
        });
        netsuiteRecord.setValue({
          fieldId: 'currency',
          value: convertHaysCurrencyToNSCurrency(invoice.currency)
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_ats_invoice_from_refund',
          value: true
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_ats_source',
          value: 2
        });
        netsuiteRecord.setValue({
          fieldId: 'custbody_refno_originvoice',
          value: invoice.correction_number
        });
        setCreditMemoInvoiceSublist_brazil(netsuiteRecord, invoice.itemList.fields);
        invoiceId = netsuiteRecord.save();
  
        record.submitFields({
          type: record.Type.INVOICE,
          id: invoiceRecord.id,
          values: {
            custbody_ats_correctioninvoice: invoiceId
          },
          options: {
            enablesourcing: false,
            ignoreMandatoryFields: true
          }
        });
  
        // Unapply all Payments from Old Invoice and apply to newly created Invoice
        var oldInvoice_rec = invoiceRecord;
        var oldInvoice_linkLineCount = oldInvoice_rec.getLineCount({ sublistId: 'links' })
        for(var i = 0; i < oldInvoice_linkLineCount; i++) {
          var linkType = oldInvoice_rec.getSublistValue({
            sublistId: 'links',
            fieldId: 'type',
            line: i
          });
          if (linkType != 'Payment') continue;
  
          var paymentId = oldInvoice_rec.getSublistValue({
            sublistId: 'links',
            fieldId: 'id',
            line: i
          });
          var payment_rec = record.load({
            type: record.Type.CUSTOMER_PAYMENT,
            id: paymentId,
            isDynamic: false
          });
          var payment_oldInvoiceLine = payment_rec.findSublistLineWithValue({
            sublistId: 'apply',
            fieldId: 'internalid',
            value: oldInvoice_rec.id.toString()
          });
          if(payment_oldInvoiceLine == -1) continue;
  
          var isApplied = payment_rec.getSublistValue({
            sublistId: 'apply',
            fieldId: 'apply',
            line: payment_oldInvoiceLine
          });
          if(!isApplied) continue;
  
          var discount = payment_rec.getSublistValue({
            sublistId: 'apply',
            fieldId: 'disc',
            line: payment_oldInvoiceLine
          });
          var paymentAmount = payment_rec.getSublistValue({
            sublistId: 'apply',
            fieldId: 'amount',
            line: payment_oldInvoiceLine
          });
  
          var payment_newInvoiceLine = payment_rec.findSublistLineWithValue({
            sublistId: 'apply',
            fieldId: 'internalid',
            value: invoiceId.toString()
          });
          if(payment_newInvoiceLine == -1) continue;
          payment_rec.setSublistValue({
            sublistId: 'apply',
            fieldId: 'apply',
            line: payment_newInvoiceLine,
            value: true
          });
          /* payment_rec.setSublistValue({
            sublistId: 'apply',
            fieldId: 'disc',
            line: payment_newInvoiceLine,
            value: discount
          });
          payment_rec.setSublistValue({
            sublistId: 'apply',
            fieldId: 'amount',
            line: payment_newInvoiceLine,
            value: paymentAmount
          }); */
          payment_rec.setSublistValue({
            sublistId: 'apply',
            fieldId: 'apply',
            line: payment_oldInvoiceLine,
            value: false
          });
          payment_rec.save();
  
          // Apply Credit Memo to Old Invoice
          var creditMemo_rec = record.load({
            type: record.Type.CREDIT_MEMO,
            id: creditMemoId,
            isDynamic: true
          });
          var applyLine_cm = creditMemo_rec.findSublistLineWithValue({
            sublistId: 'apply',
            fieldId: 'internalid',
            value: invoiceRecord.id.toString()
          });
          log.debug('creditMemoId', creditMemoId);
          log.debug('invoiceRecord.id.toString()', invoiceRecord.id.toString());
          log.debug('applyLine_cm', applyLine_cm);
          if(applyLine_cm != -1) {
            creditMemo_rec.selectLine({
              sublistId: 'apply',
              line: applyLine_cm
            });
            creditMemo_rec.setCurrentSublistValue({
              sublistId: 'apply',
              fieldId: 'apply',
              value: true,
              ignoreFieldChange: false
            });
            creditMemo_rec.commitLine({ sublistId: 'apply' });
          }
          creditMemo_rec.save();
        }
      } catch (e) {
        log.error('Error on Brazil Correction Invoice', JSON.stringify(e));
        throw nsError.create({
          message: 'Unable to create Correction Invoice: ' + e.message,
          name: 'CORRECTION_INV_NOT_CREATED',
          notifyOff: true
        })
      }
      return invoiceId;
      // updateHayEventStatus(haysEvent, STATUS.SUCCESS);
    };
    var createPayment = function(netsuiteRecord, hays, haysEvent) {
      var payment = hays.fields;
      findExistingRecord({
        externalid: payment.reference.externalid,
        recordtype: search.Type.INVOICE,
        exists: function(invoiceRecord) {
          var invoiceId = +invoiceRecord.id;
          var invoiceStatus = invoiceRecord.getValue('status');
          var createAsUnapplied = false;
  
          // Create Customer Payment as unapplied if Invoice is Fully Paid by Credit Memo or Payment (Different Ext ID)
          if(invoiceStatus == 'Paid In Full') {
            var invoiceAmount = Number(invoiceRecord.getValue('total'));
            var linkLineCount = invoiceRecord.getLineCount('links'); // Related Records > Payments
            for(var i = 0; i < linkLineCount; i++) {
              var lineType = invoiceRecord.getSublistValue({
                sublistId: 'links',
                fieldId: 'type',
                line: i
              });
              var lineTotal = Number(invoiceRecord.getSublistValue({
                sublistId: 'links',
                fieldId: 'total',
                line: i
              }));
              
              if(lineTotal == invoiceAmount && (lineType == 'Credit Memo' || lineType == 'Payment')) {
                createAsUnapplied = true;
                break;
              }
            }
  
            if(createAsUnapplied == false) throw nsError.create({
              message: 'Unable to create Payment. Invoice is already Paid in Full',
              name: 'INVOICE_ALREADY_PAID_IN_FULL',
              notifyOff: true
            });
          }
  
          // Create Customer Payment
          var subsidiaryId = '';
          if(createAsUnapplied) {
            subsidiaryId = +invoiceRecord.getValue({
              fieldId: 'subsidiary'
            });
            netsuiteRecord = record.create({
              type: record.Type.CUSTOMER_PAYMENT,
              isDynamic: true,
              defaultValues: {
                entity: invoiceRecord.getValue('entity'),
                customform: getCustomFormId(subsidiaryId, 'customerpayment'),
                subsidiary: subsidiaryId
              }
            });
            netsuiteRecord.setValue('class', invoiceRecord.getValue('class'));
            netsuiteRecord.setValue('payment', payment.payment);
          } else {
            netsuiteRecord = record.transform({
              fromType: record.Type.INVOICE,
              fromId: invoiceId,
              toType: 'customerpayment',
              isDynamic: true
            });
            subsidiaryId = +netsuiteRecord.getValue({
              fieldId: 'subsidiary'
            });
            if (+subsidiaryId > 0) {
              var formId = getCustomFormId(subsidiaryId, 'customerpayment');
              if (+formId > 0) {
                netsuiteRecord.setValue({
                  fieldId: 'customform',
                  value: formId
                });
              }
              netsuiteRecord.setValue({
                fieldId: 'subsidiary',
                value: subsidiaryId
              });
            }
          }
  
          netsuiteRecord.setValue({
            fieldId: 'externalid',
            value: "" + hays.externalid
          });
          netsuiteRecord.setValue({
            fieldId: 'memo',
            value: "" + hays.externalid
          });
          netsuiteRecord.setValue({
            fieldId: 'trandate',
            value: convertHaysDateToNSDate(payment.tranDate)
          });
          netsuiteRecord.setValue({
            fieldId: 'custbody_hays_event',
            value: +haysEvent.id
          });
          netsuiteRecord.setValue({
            fieldId: 'custbody_ats_source',
            value: 2
          });
          // Set the segments
          netsuiteRecord.setValue({
            fieldId: 'location',
            value: +invoiceRecord.getValue('location')
          });
          netsuiteRecord.setValue({
            fieldId: 'department',
            value: +invoiceRecord.getValue('department')
          });
          netsuiteRecord.setValue({
            fieldId: 'cseg_kpi_category',
            value: +invoiceRecord.getValue('cseg_kpi_category')
          });
          netsuiteRecord.setValue({
            fieldId: 'cseg_acq_channels',
            value: +invoiceRecord.getValue('cseg_acq_channels')
          });
          netsuiteRecord.setValue({
            fieldId: 'cseg_markets',
            value: +invoiceRecord.getValue('cseg_markets')
          });
          netsuiteRecord.setValue({
            fieldId: 'cseg_kpi_market',
            value: +invoiceRecord.getValue('cseg_kpi_market')
          });
          netsuiteRecord.setValue({
            fieldId: 'class',
            value: +invoiceRecord.getValue('class')
          });
          if (+invoiceRecord.getValue('custbody_mx_txn_sat_payment_term') === 3) {
            netsuiteRecord.setValue({
              fieldId: 'custbody_mx_cfdi_uuid',
              value: invoiceRecord.getValue('custbody_mx_cfdi_uuid')
            });
          } else {
            netsuiteRecord.setValue({
              fieldId: 'custbody_mx_cfdi_uuid',
              value: null
            });
          }
          if (payment.invoice_payment_method) {
            switch (+payment.invoice_payment_method) {
              case 1: // electronic wallet
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_WALLET
                });
                break;
              case 2: // electronic fund transfer
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_FUNDS_TRANSFER
                });
                break;
              case 3: // cash
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CASH
                });
                break;
              case 4: // check
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CHECK
                });
                break;
              case 5: // credit card
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.CREDIT_CARD
                });
                break;
              case 6: // electronic money
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.ELECTRONIC_MONEY
                });
                break;
              case 9: // debit card
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.DEBIT_CARD
                });
                break;
              case 99: // to be defined
                netsuiteRecord.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_method',
                  value: SAT_PAYMENT_METHOD.TO_BE_DEFINED
                });
                break;
              default:
                break;
            }
          } else {
            netsuiteRecord.setValue({
              fieldId: 'custbody_mx_txn_sat_payment_method',
              value: invoiceRecord.getValue('custbody_mx_txn_sat_payment_method')
            });
          }
          // Payment Method
          var paymentMethodId = findPaymentMethod(payment.paymentMethod, payment.provider_name);
          netsuiteRecord.setValue({
            fieldId: 'paymentmethod',
            value: paymentMethodId
          });
          var paymentGatewayId = null;
          if (payment.provider_name) {
            paymentGatewayId = findPaymentGateway(payment.provider_name, subsidiaryId);
            if (+paymentGatewayId > 0) {
              netsuiteRecord.setValue({
                fieldId: 'custbody_ats_payment_gateway',
                value: paymentGatewayId
              });
            }
          }
          netsuiteRecord.setValue({
            fieldId: 'custbody_ats_psp_reference',
            value: payment.provider_reference
          });
          // netsuiteRecord.setValue({fieldId: 'custbody_ats_payment_url', value: payment.provider_name});
          var accountMapping = null;
          if (subsidiaryId !== SUBSIDIARIES.BRAZIL && subsidiaryId !== SUBSIDIARIES.MEXICO) {
            // Set the account id
            accountMapping = findAccountId(subsidiaryId, +invoiceRecord.getValue('cseg_markets'), paymentGatewayId, null, payment.payu_3ds);
          } else {
            // if brazil / mexico
            accountMapping = findAccountId(subsidiaryId, +invoiceRecord.getValue('cseg_markets'), paymentGatewayId, paymentMethodId, payment.payu_3ds);
          }
          var accountId = accountMapping.account;
          if (+accountId > 0) {
            netsuiteRecord.setValue({
              fieldId: 'undepfunds',
              value: 'F'
            });
            netsuiteRecord.setValue({
              fieldId: 'account',
              value: accountId
            });
          }
          if (payment.payu_3ds && subsidiaryId === SUBSIDIARIES.TURKEY) {
            if (payment.payu_3ds === true) {
              netsuiteRecord.setValue({
                fieldId: 'custbody_3d_sec_enabled',
                value: true
              });
            }
          }
  
          var businessSegment = netsuiteRecord.getValue('class');
          if(isEmpty(businessSegment)) {
            var invoiceLineCount = invoiceRecord.getLineCount('item');
            for(var i = 0; i < invoiceLineCount; i++) {
              var lineBusinessSegment = invoiceRecord.getSublistValue({
                sublistId: 'item',
                fieldId: 'class',
                line: i
              });
              if(isEmpty(lineBusinessSegment)) continue;
              businessSegment = lineBusinessSegment;
              netsuiteRecord.setValue('class', businessSegment);
              break;
            }
          }
          if(isEmpty(businessSegment)) netsuiteRecord.setValue('class', PYMT_DFLT_BUSSEGMENT);
  
          var sublistId = 'apply';
          var i = netsuiteRecord.getLineCount({
            sublistId: sublistId
          }) - 1;
          while (i > 0 || i == 0) {
            netsuiteRecord.selectLine({
              sublistId: sublistId,
              line: i
            });
            var refNumber = +netsuiteRecord.getCurrentSublistValue({
              sublistId: sublistId,
              fieldId: 'internalid'
            });
            if (refNumber == invoiceId) {
              netsuiteRecord.setCurrentSublistValue({
                sublistId: sublistId,
                fieldId: 'apply',
                value: true
              });
              netsuiteRecord.setCurrentSublistValue({
                sublistId: sublistId,
                fieldId: 'amount',
                value: +payment.payment
              });
              netsuiteRecord.commitLine({
                sublistId: sublistId
              });
            }
            i = i - 1;
          }
          netsuiteRecord.save();
          // updateHayEventStatus(haysEvent, STATUS.SUCCESS);
        },
        doesnotexist: function() {
          throw nsError.create({
            message: "Invoice Not Found, Externalid - Externalid - " + payment.reference.externalid,
            name: 'INVOICE_NOT_FOUND',
            notifyOff: false
          });
        },
        internalIdOnly: false
      });
    };
    var createCreditMemo = function(creditMemo, hays, haysEvent, invExtId) {
      //log.error('INSIDE THE CREATECREDITMEMO');
      var credit = hays.fields;
      var invoiceExternalId = isEmpty(hays.reference) ? invExtId : hays.reference.externalid
      findExistingRecord({
        externalid: invoiceExternalId,
        recordtype: search.Type.INVOICE,
        exists: function(invoiceRecord) {
          var subsidiaryId = +invoiceRecord.getValue({
            fieldId: 'subsidiary'
          });
          var formId = getCustomFormId(subsidiaryId, 'creditmemo');
          creditMemo = record.transform({
            fromType: record.Type.INVOICE,
            fromId: +invoiceRecord.id,
            toType: record.Type.CREDIT_MEMO,
            isDynamic: true,
            defaultValues: {
              customform: formId
            }
          });
          var tranidText = "" + invoiceRecord.getValue('tranid');
          if (subsidiaryId === SUBSIDIARIES.SPAIN) {
            creditMemo.setValue({
              fieldId: 'tranid',
              value: tranidText + "cm"
            });
            creditMemo.setValue({
              fieldId: 'externalid',
              value: "" + hays.externalid + "cm"
            });
            creditMemo.setValue({
              fieldId: 'memo',
              value: "" + hays.externalid + "cm"
            });
          } else if (subsidiaryId === SUBSIDIARIES.BRAZIL) {
            creditMemo.setValue({
              fieldId: 'tranid',
              value: tranidText + "CM"
            });
            creditMemo.setValue({
              fieldId: 'externalid',
              value: "" + hays.externalid + "CM"
            });
            creditMemo.setValue({
              fieldId: 'memo',
              value: "" + hays.externalid + "CM"
            });
          } else {
            creditMemo.setValue({
              fieldId: 'tranid',
              value: "" + credit.correction_number
            });
            creditMemo.setValue({
              fieldId: 'externalid',
              value: "" + hays.externalid
            });
            creditMemo.setValue({
              fieldId: 'memo',
              value: "" + hays.externalid
            });
          }
  
          if (credit.uuid) {
            creditMemo.setValue({
              fieldId: 'custbody_mx_cfdi_uuid',
              value: credit.uuid
            });
          }
          if (credit.terms) {
            switch (credit.terms) {
              case 'PUE':
                creditMemo.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_term',
                  value: 3
                });
                break;
              case 'PPD':
                creditMemo.setValue({
                  fieldId: 'custbody_mx_txn_sat_payment_term',
                  value: 4
                });
                break;
              default:
                break;
            }
          }
          creditMemo.setValue({
            fieldId: 'custbody_ats_source',
            value: 2
          });
          creditMemo.setValue({
            fieldId: 'custbody_hays_event',
            value: +haysEvent.id
          });
          creditMemo.setValue({
            fieldId: 'trandate',
            value: convertHaysDateToNSDate(credit.trandate)
          });
          // Set the segments
          creditMemo.setValue({
            fieldId: 'location',
            value: +invoiceRecord.getValue('location')
          });
          creditMemo.setValue({
            fieldId: 'department',
            value: +invoiceRecord.getValue('department')
          });
          creditMemo.setValue({
            fieldId: 'cseg_kpi_category',
            value: +invoiceRecord.getValue('cseg_kpi_category')
          });
          creditMemo.setValue({
            fieldId: 'cseg_acq_channels',
            value: +invoiceRecord.getValue('cseg_acq_channels')
          });
          creditMemo.setValue({
            fieldId: 'cseg_markets',
            value: +invoiceRecord.getValue('cseg_markets')
          });
          creditMemo.setValue({
            fieldId: 'cseg_kpi_market',
            value: +invoiceRecord.getValue('cseg_kpi_market')
          });
  
          if(!isEmpty(credit.itemList)) {
            if (+creditMemo.getValue('subsidiary') === SUBSIDIARIES.BRAZIL) {
              // setCreditMemoSublist_brazil(creditMemo, credit.itemList.fields, +invoiceRecord.id);
            } else if (+creditMemo.getValue('subsidiary') === SUBSIDIARIES.SPAIN) {
              // setCreditMemoSublist(creditMemo, credit.itemList.fields, +invoiceRecord.id);
            }
            else if (+creditMemo.getValue('subsidiary') === SUBSIDIARIES.MEXICO){
              setCreditMemoSublist_mexico(creditMemo, credit.itemList.fields, +invoiceRecord.id);
            }
            else {
              setCreditMemoSublist(creditMemo, credit.itemList.fields, +invoiceRecord.id);
            }
          }
          var creditMemoId = creditMemo.save();
          if (+creditMemo.getValue('subsidiary') === SUBSIDIARIES.SPAIN) {
            createInvoiceFromRefund(invoiceRecord, hays, haysEvent);
          } else if (+creditMemo.getValue('subsidiary') === SUBSIDIARIES.BRAZIL) {
            createInvoiceFromRefund_brazil(invoiceRecord, hays, haysEvent, creditMemoId);
          }
  
          // Unapply Payment and Apply Credit Memo
          if(+creditMemo.getValue('subsidiary') === SUBSIDIARIES.ITALY) {
            var creditMemo_fields = search.lookupFields({
              type: search.Type.CREDIT_MEMO,
              id: creditMemoId,
              columns: ['total']
            })
            var creditMemo_total = Math.abs(Number(creditMemo_fields.total));
  
            // Get Payment from Invoice with same total
            var invLinkLineCount = invoiceRecord.getLineCount({ sublistId: 'links' });
            var paymentId = '';
            for(var i = 0; i < invLinkLineCount; i++) {
              var linkType = invoiceRecord.getSublistValue({
                sublistId: 'links',
                fieldId: 'type',
                line: i
              });
              if (linkType != 'Payment') continue;
              var paymentAmount =  Math.abs(Number(invoiceRecord.getSublistValue({
                sublistId: 'links',
                fieldId: 'total',
                line: i
              })));
              if(creditMemo_total != paymentAmount) continue;
  
              paymentId = invoiceRecord.getSublistValue({
                sublistId: 'links',
                fieldId: 'id',
                line: i
              })
              break;
            }
  
            // If No Payment with same Total found, get first Payment
            if(isEmpty(paymentId)) {
              for(var i = 0; i < invLinkLineCount; i++) {
                var linkType = invoiceRecord.getSublistValue({
                  sublistId: 'links',
                  fieldId: 'type',
                  line: i
                });
                if (linkType != 'Payment') continue;
    
                paymentId = invoiceRecord.getSublistValue({
                  sublistId: 'links',
                  fieldId: 'id',
                  line: i
                })
                break;
              }
            }
            
            if(isEmpty(paymentId)) throw error.create({
              message: 'Credit Memo was created but no Payment was found on the Invoice.',
              name: 'NO_PAYMENT_FOUND',
              notifyOff: true
            })
            // Unapply Payment
            var payment_rec = record.load({
              type: record.Type.CUSTOMER_PAYMENT,
              id: paymentId,
              isDynamic: true
            });
            var paymentLineCount = payment_rec.getLineCount({ sublistId: 'apply' });
            for(var i = 0; i < paymentLineCount; i++) {
              var line_invoiceId = payment_rec.getSublistValue({
                sublistId: 'apply',
                fieldId: 'internalid',
                line: i
              })
              log.debug('line_invoiceId.toString()', line_invoiceId.toString());
              log.debug('invoiceRecord.id.toString()', invoiceRecord.id.toString());
              if(line_invoiceId.toString() != invoiceRecord.id.toString()) continue;
  
              payment_rec.selectLine({
                sublistId: 'apply',
                line: i
              });
              payment_rec.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: false,
              });
              payment_rec.commitLine({ sublistId: 'apply' })
              break;
            }
            payment_rec.save();
  
            // Apply Credit Memo
            var creditMemo_rec = record.load({
              type: record.Type.CREDIT_MEMO,
              id: creditMemoId,
              isDynamic: true
            })
            var creditMemoLineCount = creditMemo_rec.getLineCount({ sublistId: 'apply' });
            for(var i = 0; i < creditMemoLineCount; i++) {
              var line_invoiceId = creditMemo_rec.getSublistValue({
                sublistId: 'apply',
                fieldId: 'internalid',
                line: i
              })
              if(line_invoiceId.toString() != invoiceRecord.id.toString()) continue;
  
              creditMemo_rec.selectLine({
                sublistId: 'apply',
                line: i
              });
              creditMemo_rec.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
  
              creditMemo_rec.commitLine({ sublistId: 'apply' })
              break;
            }
            creditMemo_rec.save();
          }
        },
        doesnotexist: function() {
          throw nsError.create({
            message: "Reference Not Found: Externalid - " + invoiceExternalId,
            name: 'REF_NOT_FOUND',
            notifyOff: false
          });
        },
        internalIdOnly: false
      });
    };
    var SUBSIDIARIES;
    (function(SUBSIDIARIES) {
      SUBSIDIARIES[SUBSIDIARIES["ITALY"] = 6] = "ITALY";
      SUBSIDIARIES[SUBSIDIARIES["TURKEY"] = 11] = "TURKEY";
      SUBSIDIARIES[SUBSIDIARIES["BRAZIL"] = 5] = "BRAZIL";
      SUBSIDIARIES[SUBSIDIARIES["SPAIN"] = 10] = "SPAIN";
      SUBSIDIARIES[SUBSIDIARIES["MEXICO"] = 8] = "MEXICO";
      SUBSIDIARIES[SUBSIDIARIES["TUOTEMPO"] = 7] = "TUOTEMPO";
      SUBSIDIARIES[SUBSIDIARIES["GIPO"] = 13] = "GIPO";
      SUBSIDIARIES[SUBSIDIARIES["POLAND"] = 9] = "POLAND";
    })(SUBSIDIARIES || (SUBSIDIARIES = {}));
    var createCashBack = function(cashBack, hays, haysEvent) {
      var credit = hays.fields;
      findExistingRecord({
        externalid: hays.reference.externalid,
        recordtype: search.Type.INVOICE,
        exists: function(invoiceRecord) {
          cashBack = record.transform({
            fromType: record.Type.INVOICE,
            fromId: +invoiceRecord.id,
            toType: record.Type.CREDIT_MEMO,
            isDynamic: true
          });
          var cashBackTranid = getNextTransactionId("" + invoiceRecord.getValue('tranid'));
          cashBack.setValue({
            fieldId: 'tranid',
            value: cashBackTranid
          });
          cashBack.setValue({
            fieldId: 'externalid',
            value: "" + hays.externalid
          });
          cashBack.setValue({
            fieldId: 'memo',
            value: "" + hays.externalid
          });
          cashBack.setValue({
            fieldId: 'custbody_hays_event',
            value: +haysEvent.id
          });
          // Set the segments
          cashBack.setValue({
            fieldId: 'location',
            value: +invoiceRecord.getValue('location')
          });
          cashBack.setValue({
            fieldId: 'department',
            value: +invoiceRecord.getValue('department')
          });
          cashBack.setValue({
            fieldId: 'cseg_kpi_category',
            value: +invoiceRecord.getValue('cseg_kpi_category')
          });
          cashBack.setValue({
            fieldId: 'cseg_acq_channels',
            value: +invoiceRecord.getValue('cseg_acq_channels')
          });
          cashBack.setValue({
            fieldId: 'cseg_markets',
            value: +invoiceRecord.getValue('cseg_markets')
          });
          cashBack.setValue({
            fieldId: 'cseg_kpi_market',
            value: +invoiceRecord.getValue('cseg_kpi_market')
          });
          var arAccountId = +findARAccountId(+invoiceRecord.getValue('subsidiary'), +invoiceRecord.getValue('cseg_markets')).araccount;
          if (+arAccountId > 0) {
            cashBack.setValue({
              fieldId: 'araccount',
              value: arAccountId
            });
          }
          cashBack.save();
          // If subsidiary = spain, create an invoice
          if (+cashBack.getValue({
              fieldId: 'subsidiary'
            }) === SUBSIDIARIES.SPAIN) {
            createInvoiceFromRefund(invoiceRecord, hays, haysEvent);
          }
        },
        doesnotexist: function() {
          log.error('Reference Not Found', "Externalid - " + hays.reference.externalid);
        },
        internalIdOnly: false
      });
    };
    var getNextTransactionId = function(tranid) {
      var invoiceNumber = tranid;
      try {
        invoiceNumber = "" + (parseInt(tranid, 10) + 1);
        invoiceNumber = Array(Math.max(tranid.length - String(invoiceNumber).length + 1, 0)).join('0') + invoiceNumber;
      } catch (e) {
        log.error('error', e);
      }
      return invoiceNumber;
    };
    var createCustomerRefund = function(customerRefund, hays, haysEvent) {
      var refund = hays.fields;
      if(refund.subsidiary.externalId == 'Doctoralia Mexico, S.A. de C.V.') {
        var invoiceId = getTransactionInternalId(refund.reference.externalid);
        var invoice_fields = search.lookupFields({
          type: search.Type.INVOICE,
          id: invoiceId,
          columns: ['custbody_ats_correctioninvoice', 'externalid']
        });
        var correctionInvoice = invoice_fields.custbody_ats_correctioninvoice;
        var correctionInvoiceId = isEmpty(correctionInvoice) ? '' : correctionInvoice[0].value;
        var correctionInvoiceExtId = '';
        if(!isEmpty(correctionInvoiceId)) {
          var correctionInvoice_fields = search.lookupFields({
            type: search.Type.INVOICE,
            id: correctionInvoiceId,
            columns: ['externalid']
          });
          correctionInvoiceExtId = isEmpty(correctionInvoice_fields.externalid) ? '' : correctionInvoice_fields.externalid[0].value;
        }
        findExistingRecord({
          externalid: refund.reference.externalid,
          recordtype: search.Type.CUSTOMER_PAYMENT,
          filters: [
            ['appliedtotransaction.externalidstring', 'is', refund.reference.externalid]
          ],
          exists: function(payment_rec) {
            // Unapply Payment
            var paymentLineCount = payment_rec.getLineCount('apply');
            log.error('PASS 0 - paymentLineCount', paymentLineCount);
            for(var i = 0; i < paymentLineCount; i++) {
              var paymentAmount = Number(payment_rec.getSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                line: i
              }));
              log.error('1ST CHECK', paymentAmount + ' | ' + (Number(refund.total) * -1));
              if(paymentAmount != (Number(refund.total) * -1)){
                payment_rec.selectLine({
                  sublistId: 'apply',
                  line: i
                })
                payment_rec.setCurrentSublistValue({
                  sublistId: 'apply',
                  fieldId: 'apply',
                  value: false
                });
                payment_rec.commitLine({ sublistId: 'apply' })
                log.error('payment_rec CHECKER', payment_rec.getSublistValue({ sublistId: 'apply', fieldId: 'apply', line: i}));
                payment_rec.save({
                  enableSourcing: false,
                  ignoreMandatoryFields: true
                });
                log.error('PASS 0.5 - payment_rec save', payment_rec);
                i = paymentLineCount; //terminator
              }
              else{
                break;
              }
            }
  
            // Create Customer Refund
            var subsidiaryId = +payment_rec.getValue('subsidiary');
            var formId = getCustomFormId(subsidiaryId, 'customerrefund');
            customerRefund = record.create({
              type: record.Type.CUSTOMER_REFUND,
              isDynamic: true,
              defaultValues: {
                customform: formId,
                entity: payment_rec.getValue('customer')
              }
            });
            var paymentMethodId = findPaymentMethod(refund.paymentMethod, refund.provider_name);
            customerRefund.setValue('paymentmethod', paymentMethodId);
            var paymentGatewayId = null;
            if (refund.provider_name) {
              paymentGatewayId = findPaymentGateway(refund.provider_name, subsidiaryId);
              if (+paymentGatewayId > 0) {
                customerRefund.setValue({
                  fieldId: 'custbody_ats_payment_gateway',
                  value: paymentGatewayId
                });
              }
            }
            if (refund.transactionNumber) customerRefund.setValue('tranid', refund.transactionNumber);
            customerRefund.setValue('custbody_ats_psp_reference', refund.provider_reference);
            customerRefund.setValue('externalid', "" + hays.externalid);
            customerRefund.setValue('memo', "" + hays.externalid);
            customerRefund.setValue('custbody_hays_event', +haysEvent.id);
            customerRefund.setValue('custbody_ats_source', 2);
            customerRefund.setValue('trandate', convertHaysDateToNSDate(refund.tranDate));
            // Set the segments
            customerRefund.setValue('location', +payment_rec.getValue('location'));
            customerRefund.setValue('department', +payment_rec.getValue('department'));
            customerRefund.setValue('class', +payment_rec.getValue('class'));
            customerRefund.setValue('cseg_kpi_category', +payment_rec.getValue('cseg_kpi_category'));
            customerRefund.setValue('cseg_acq_channels', +payment_rec.getValue('cseg_acq_channels'));
            customerRefund.setValue('cseg_markets', +payment_rec.getValue('cseg_markets'));
            customerRefund.setValue('cseg_kpi_market', +payment_rec.getValue('cseg_kpi_market'));
            var accountMapping = null;
            if (subsidiaryId !== SUBSIDIARIES.BRAZIL && subsidiaryId !== SUBSIDIARIES.MEXICO) {
              // Set the account id
              accountMapping = findAccountId(subsidiaryId, +payment_rec.getValue('cseg_markets'), paymentGatewayId);
            } else {
              // if brazil
              accountMapping = findAccountId(subsidiaryId, +payment_rec.getValue('cseg_markets'), paymentGatewayId, paymentMethodId);
            }
            var accountId = accountMapping.account;
            if (+accountId > 0) {
              customerRefund.setValue('undepfunds', 'F');
              customerRefund.setValue('account', accountId);
            }
            var refundLineToApply = customerRefund.findSublistLineWithValue({
              sublistId: 'apply',
              fieldId: 'internalid',
              value: payment_rec.id.toString()
            });
            log.error('PASS 6 - refundLineToApply', refundLineToApply);
            if(refundLineToApply != -1) {
              customerRefund.selectLine({
                sublistId: 'apply',
                line: refundLineToApply
              });
              customerRefund.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
              customerRefund.commitLine({ sublistId: 'apply' })
            }
            //return +customerRefund.save();
            var custRefundID = +customerRefund.save();
  
            // Check for Credit Memo on Invoice = EXPERIMENTAL ADDITION - raph
            if(refund.subsidiary.externalId) {
              var creditMemoId = getCreditMemoByInvoice(invoiceId);
              // Create Credit Memo
              if(isEmpty(creditMemoId)) {
                var creditMemoSubsidiaryId = payment_rec.getValue('subsidiary');
                var creditMemoFormId = getCustomFormId(creditMemoSubsidiaryId, 'creditmemo');
                var creditMemo_rec = record.transform({
                  fromType: record.Type.INVOICE,
                  fromId: invoiceId,
                  toType: record.Type.CREDIT_MEMO,
                  isDynamic: true,
                  defaultValues: {
                    customform: creditMemoFormId
                  }
                });
                creditMemo_rec.setValue('custbody_hays_event', +haysEvent.id);
                creditMemo_rec.setValue('custbody_ats_source', 2);
                creditMemo_rec.setValue('trandate', convertHaysDateToNSDate(refund.tranDate));
  
                var creditMemoLineCount = creditMemo_rec.getLineCount('item');
                for(var i = 0; i < creditMemoLineCount; i++) {
                  var cmLineBusinessSegment = creditMemo_rec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'class',
                    line: i
                  });
                  if(!isEmpty(cmLineBusinessSegment)) continue;
  
                  var cmLineItemId = creditMemo_rec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'item',
                    line: i
                  });
                  var item_fields = search.lookupFields({
                    type: search.Type.ITEM,
                    id: cmLineItemId,
                    columns: ['class', 'itemid']
                  });
                  if(isEmpty(item_fields.class)) throw nsError.create({
                    message: 'No Business Segment found on Item ' + item_fields.itemid,
                    name: 'NO_BUSINESS_SEGMENT_ON_ITEM',
                    notifyOff: false
                  });
                  
                  creditMemo_rec.selectLine({
                    sublistId: 'item',
                    line: i
                  });
                  creditMemo_rec.setCurrentSublistValue({
                    sublistId: 'item',
                    fieldId: 'class',
                    value: item_fields.class[0].value
                  });
                  creditMemo_rec.commitLine({ sublistId: 'item' })
                }
                creditMemo_rec.save();
              } else {
                var creditMemo_rec = record.load({
                  type: record.Type.CREDIT_MEMO,
                  id: creditMemoId,
                  isDynamic: true
                });
                var applyLineCount = creditMemo_rec.getLineCount('apply');
                for(var i = 0; i < applyLineCount; i++) {
                  var tranLineId = creditMemo_rec.getSublistValue({
                    sublistId: 'apply',
                    fieldId: 'internalid',
                    line: i
                  })
                  if(tranLineId != invoiceId) continue;
  
                  creditMemo_rec.selectLine({
                    sublistId: 'apply',
                    line: i
                  });
                  creditMemo_rec.setCurrentSublistValue({
                    sublistId: 'apply',
                    fieldId: 'apply',
                    value: true
                  });
                  creditMemo_rec.commitLine({ sublistId: 'apply' })
                  break;
                }
                creditMemo_rec.save();
              }
            } //END EXPERIMENTAL ADDITION - raph
          },
          doesnotexist: function() {
            log.error('Reference Not Found', "Externalid - " + refund.reference.externalid);
            throw nsError.create({
              message: 'No Payment Found for Invoice with External ID of ' + refund.reference.externalid,
              name: 'NO_PAYMENT_FOUND',
              notifyOff: true
            })
          },
          internalIdOnly: false
        });
      }
      else if(refund.subsidiary.externalId == 'Doctoralia Brasil Serviços Online e Software Ltda' || refund.subsidiary.externalId == 'Doctoralia Internet, S.L.') {
        var invoiceId = getTransactionInternalId(refund.reference.externalid);
        var invoice_fields = search.lookupFields({
          type: search.Type.INVOICE,
          id: invoiceId,
          columns: ['custbody_ats_correctioninvoice', 'externalid']
        });
        var correctionInvoice = invoice_fields.custbody_ats_correctioninvoice;
        var correctionInvoiceId = isEmpty(correctionInvoice) ? '' : correctionInvoice[0].value;
        var correctionInvoiceExtId = '';
        if(!isEmpty(correctionInvoiceId)) {
          var correctionInvoice_fields = search.lookupFields({
            type: search.Type.INVOICE,
            id: correctionInvoiceId,
            columns: ['externalid']
          });
          correctionInvoiceExtId = isEmpty(correctionInvoice_fields.externalid) ? '' : correctionInvoice_fields.externalid[0].value;
        }
        findExistingRecord({
          externalid: refund.reference.externalid,
          recordtype: search.Type.CUSTOMER_PAYMENT,
          filters: [
            ['appliedtotransaction.externalidstring', 'is', refund.reference.externalid]
          ],
          exists: function(payment_rec) {
  
            // Unapply Payment
            var paymentBusinessSegment = payment_rec.getValue('class');
            if(isEmpty(paymentBusinessSegment)) {
              var invoice_rec = record.load({
                type: record.Type.INVOICE,
                id: invoiceId,
                isDynamic: false
              });
              var invoiceLineCount = invoice_rec.getLineCount('item');
              var isBusSegmentSet = false
              for(var i = 0; i < invoiceLineCount; i++) {
                var invoiceLineBusinessSegment = invoice_rec.getSublistValue({
                  sublistId: 'item',
                  fieldId: 'class',
                  line: i
                });
                if(isEmpty(invoiceLineBusinessSegment)) continue;
  
                payment_rec.setValue('class', invoiceLineBusinessSegment);
                isBusSegmentSet = true;
                break;
              }
  
              if(!isBusSegmentSet) {
                for(var i = 0; i < invoiceLineCount; i++) {
                  var invLineItemId = invoice_rec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'item',
                    line: i
                  });
                  var item_fields = search.lookupFields({
                    type: search.Type.ITEM,
                    id: invLineItemId,
                    columns: ['class', 'itemid']
                  });
                  if(isEmpty(item_fields.class)) continue;
    
                  payment_rec.setValue('class', item_fields.class[0].value);
                  isBusSegmentSet = true;
                  break;
                }
                if(!isBusSegmentSet) throw nsError.create({
                  message: 'No Business Segment Found on the Items',
                  name: 'NO_BUSINESS_SEGMENT_FOUND',
                  notifyOff: false
                });
              }
            }
  
            var paymentLineCount = payment_rec.getLineCount('apply');
            var isPaymentUnapplied = false;
            for(var i = 0; i < paymentLineCount; i++) {
              var paymentAmount = Number(payment_rec.getSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                line: i
              }));
              if(paymentAmount != Number(refund.total) * -1) continue;
              payment_rec.selectLine({
                sublistId: 'apply',
                line: i
              })
              payment_rec.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: false
              });
              payment_rec.commitLine({ sublistId: 'apply' })
              isPaymentUnapplied = true;
              break;
            }
            /* if(!isPaymentUnapplied) {
              for(var i = 0; i < paymentLineCount; i++) {
                var paymentAmount = Number(payment_rec.getSublistValue({
                  sublistId: 'apply',
                  fieldId: 'amount',
                  line: i
                }));
                var apply_invoiceId = Number(payment_rec.getSublistValue({
                  sublistId: 'apply',
                  fieldId: 'internalid',
                  line: i
                }));
                if(apply_invoiceId != invoiceId) continue;
                payment_rec.selectLine({
                  sublistId: 'apply',
                  line: i
                })
                payment_rec.setCurrentSublistValue({
                  sublistId: 'apply',
                  fieldId: 'amount',
                  value: paymentAmount + Number(refund.total)
                });
                payment_rec.commitLine({ sublistId: 'apply' })
                break;
              }
            }*/
            payment_rec.save({
              enableSourcing: false,
              ignoreMandatoryFields: true
            });
  
            // Create Customer Refund
            var subsidiaryId = +payment_rec.getValue('subsidiary');
            var formId = getCustomFormId(subsidiaryId, 'customerrefund');
            customerRefund = record.create({
              type: record.Type.CUSTOMER_REFUND,
              isDynamic: true,
              defaultValues: {
                customform: formId,
                entity: payment_rec.getValue('customer')
              }
            });
            var paymentMethodId = findPaymentMethod(refund.paymentMethod, refund.provider_name);
            customerRefund.setValue('paymentmethod', paymentMethodId);
            var paymentGatewayId = null;
            if (refund.provider_name) {
              paymentGatewayId = findPaymentGateway(refund.provider_name, subsidiaryId);
              if (+paymentGatewayId > 0) {
                customerRefund.setValue({
                  fieldId: 'custbody_ats_payment_gateway',
                  value: paymentGatewayId
                });
              }
            }
            if (refund.transactionNumber) customerRefund.setValue('tranid', refund.transactionNumber);
            customerRefund.setValue('custbody_ats_psp_reference', refund.provider_reference);
            customerRefund.setValue('externalid', "" + hays.externalid);
            customerRefund.setValue('memo', "" + hays.externalid);
            customerRefund.setValue('custbody_hays_event', +haysEvent.id);
            customerRefund.setValue('custbody_ats_source', 2);
            customerRefund.setValue('trandate', convertHaysDateToNSDate(refund.tranDate));
            // Set the segments
            customerRefund.setValue('location', +payment_rec.getValue('location'));
            customerRefund.setValue('department', +payment_rec.getValue('department'));
            customerRefund.setValue('class', +payment_rec.getValue('class'));
            customerRefund.setValue('cseg_kpi_category', +payment_rec.getValue('cseg_kpi_category'));
            customerRefund.setValue('cseg_acq_channels', +payment_rec.getValue('cseg_acq_channels'));
            customerRefund.setValue('cseg_markets', +payment_rec.getValue('cseg_markets'));
            customerRefund.setValue('cseg_kpi_market', +payment_rec.getValue('cseg_kpi_market'));
            var accountMapping = null;
            if (subsidiaryId !== SUBSIDIARIES.BRAZIL && subsidiaryId !== SUBSIDIARIES.MEXICO) {
              // Set the account id
              accountMapping = findAccountId(subsidiaryId, +payment_rec.getValue('cseg_markets'), paymentGatewayId);
            } else {
              // if brazil
              accountMapping = findAccountId(subsidiaryId, +payment_rec.getValue('cseg_markets'), paymentGatewayId, paymentMethodId);
            }
            var accountId = accountMapping.account;
            if (+accountId > 0) {
              customerRefund.setValue('undepfunds', 'F');
              customerRefund.setValue('account', accountId);
            }
  
            var creditApplyLineCount = customerRefund.getLineCount('apply');
            log.debug('creditApplyLineCount', creditApplyLineCount);
            for(var i = 0; i < creditApplyLineCount; i++) {
              var tranLineId = customerRefund.getSublistValue({
                sublistId: 'apply',
                fieldId: 'internalid',
                line: i
              })
              log.debug('tranLineId - payment_rec.id.toString()', tranLineId + ' - ' + payment_rec.id.toString());
              if(tranLineId != payment_rec.id.toString()) continue;
  
              customerRefund.selectLine({
                sublistId: 'apply',
                line: i
              });
              customerRefund.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'apply',
                value: true
              });
              customerRefund.setCurrentSublistValue({
                sublistId: 'apply',
                fieldId: 'amount',
                value: Math.abs(Number(refund.total))
              });
              customerRefund.commitLine({ sublistId: 'apply' })
            }
            var customerRefundId = +customerRefund.save();
  
            // Check for Credit Memo on Invoice
            if(refund.subsidiary.externalId != 'Doctoralia Brasil Serviços Online e Software Ltda') {
              var creditMemoId = getCreditMemoByInvoice(invoiceId);
              // Create Credit Memo
              if(isEmpty(creditMemoId)) {
                var creditMemoSubsidiaryId = payment_rec.getValue('subsidiary');
                var creditMemoFormId = getCustomFormId(creditMemoSubsidiaryId, 'creditmemo');
                var creditMemo_rec = record.transform({
                  fromType: record.Type.INVOICE,
                  fromId: invoiceId,
                  toType: record.Type.CREDIT_MEMO,
                  isDynamic: true,
                  defaultValues: {
                    customform: creditMemoFormId
                  }
                });
                creditMemo_rec.setValue('custbody_hays_event', +haysEvent.id);
                creditMemo_rec.setValue('custbody_ats_source', 2);
                creditMemo_rec.setValue('trandate', convertHaysDateToNSDate(refund.tranDate));
  
                var creditMemoLineCount = creditMemo_rec.getLineCount('item');
                for(var i = 0; i < creditMemoLineCount; i++) {
                  var cmLineBusinessSegment = creditMemo_rec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'class',
                    line: i
                  });
                  if(!isEmpty(cmLineBusinessSegment)) continue;
  
                  var cmLineItemId = creditMemo_rec.getSublistValue({
                    sublistId: 'item',
                    fieldId: 'item',
                    line: i
                  });
                  var item_fields = search.lookupFields({
                    type: search.Type.ITEM,
                    id: cmLineItemId,
                    columns: ['class', 'itemid']
                  });
                  if(isEmpty(item_fields.class)) throw nsError.create({
                    message: 'No Business Segment found on Item ' + item_fields.itemid,
                    name: 'NO_BUSINESS_SEGMENT_ON_ITEM',
                    notifyOff: false
                  });
                  
                  creditMemo_rec.selectLine({
                    sublistId: 'item',
                    line: i
                  });
                  creditMemo_rec.setCurrentSublistValue({
                    sublistId: 'item',
                    fieldId: 'class',
                    value: item_fields.class[0].value
                  });
                  creditMemo_rec.commitLine({ sublistId: 'item' })
                }
                creditMemo_rec.save();
              } else {
                var creditMemo_rec = record.load({
                  type: record.Type.CREDIT_MEMO,
                  id: creditMemoId,
                  isDynamic: true
                });
                var applyLineCount = creditMemo_rec.getLineCount('apply');
                for(var i = 0; i < applyLineCount; i++) {
                  var tranLineId = creditMemo_rec.getSublistValue({
                    sublistId: 'apply',
                    fieldId: 'internalid',
                    line: i
                  })
                  if(tranLineId != invoiceId) continue;   //probably causing the issue?
  
                  creditMemo_rec.selectLine({
                    sublistId: 'apply',
                    line: i
                  });
                  creditMemo_rec.setCurrentSublistValue({
                    sublistId: 'apply',
                    fieldId: 'apply',
                    value: true
                  });
                  creditMemo_rec.commitLine({ sublistId: 'apply' })
                  break;
                }
                creditMemo_rec.save();
              }
            }
  
            return customerRefundId;
          },
          doesnotexist: function() {
            if(isEmpty(correctionInvoiceExtId)) {
              log.error('Reference Not Found', "Externalid - " + refund.reference.externalid);
              throw nsError.create({
                message: 'No Payment Found for Invoice with External ID of ' + refund.reference.externalid,
                name: 'NO_PAYMENT_FOUND',
                notifyOff: true
              })
            } else {
              hays.fields.reference.externalid = correctionInvoiceExtId;
              createCustomerRefund(customerRefund, hays, haysEvent);
            }
          },
          internalIdOnly: false
        });
      }
      else {
        findExistingRecord({
          externalid: refund.reference.externalid,
          recordtype: search.Type.CREDIT_MEMO,
          filters: [
            ['createdfrom.externalidstring', 'is', refund.reference.externalid]
          ],
          exists: function(creditMemoRecord) {
            var subsidiaryId = +creditMemoRecord.getValue({
              fieldId: 'subsidiary'
            });
            var formId = getCustomFormId(subsidiaryId, 'customerrefund');
            // If credit memo exists, transform it
            customerRefund = record.create({
              type: record.Type.CUSTOMER_REFUND,
              isDynamic: true,
              defaultValues: {
                cred: creditMemoRecord.id,
                customform: formId
              }
            });
            var paymentMethodId = findPaymentMethod(refund.paymentMethod, refund.provider_name);
            customerRefund.setValue({
              fieldId: 'paymentmethod',
              value: paymentMethodId
            });
            var paymentGatewayId = null;
            if (refund.provider_name) {
              paymentGatewayId = findPaymentGateway(refund.provider_name, subsidiaryId);
              if (+paymentGatewayId > 0) {
                customerRefund.setValue({
                  fieldId: 'custbody_ats_payment_gateway',
                  value: paymentGatewayId
                });
              }
            }
            if (refund.transactionNumber) {
              customerRefund.setValue({
                fieldId: 'tranid',
                value: refund.transactionNumber
              });
            }
            customerRefund.setValue({
              fieldId: 'custbody_ats_psp_reference',
              value: refund.provider_reference
            });
            customerRefund.setValue({
              fieldId: 'externalid',
              value: "" + hays.externalid
            });
            customerRefund.setValue({
              fieldId: 'memo',
              value: "" + hays.externalid
            });
            customerRefund.setValue({
              fieldId: 'custbody_hays_event',
              value: +haysEvent.id
            });
            customerRefund.setValue({
              fieldId: 'custbody_ats_source',
              value: 2
            });
            customerRefund.setValue({
              fieldId: 'trandate',
              value: convertHaysDateToNSDate(refund.tranDate)
            });
            // Set the segments
            customerRefund.setValue({
              fieldId: 'location',
              value: +creditMemoRecord.getValue('location')
            });
            customerRefund.setValue({
              fieldId: 'department',
              value: +creditMemoRecord.getValue('department')
            });
            customerRefund.setValue({
              fieldId: 'cseg_kpi_category',
              value: +creditMemoRecord.getValue('cseg_kpi_category')
            });
            customerRefund.setValue({
              fieldId: 'cseg_acq_channels',
              value: +creditMemoRecord.getValue('cseg_acq_channels')
            });
            customerRefund.setValue({
              fieldId: 'cseg_markets',
              value: +creditMemoRecord.getValue('cseg_markets')
            });
            customerRefund.setValue({
              fieldId: 'cseg_kpi_market',
              value: +creditMemoRecord.getValue('cseg_kpi_market')
            });
            var accountMapping = null;
            if (subsidiaryId !== SUBSIDIARIES.BRAZIL && subsidiaryId !== SUBSIDIARIES.MEXICO) {
              // Set the account id
              accountMapping = findAccountId(subsidiaryId, +creditMemoRecord.getValue('cseg_markets'), paymentGatewayId);
            } else {
              // if brazil
              accountMapping = findAccountId(subsidiaryId, +creditMemoRecord.getValue('cseg_markets'), paymentGatewayId, paymentMethodId);
            }
            var accountId = accountMapping.account;
            if (+accountId > 0) {
              customerRefund.setValue({
                fieldId: 'undepfunds',
                value: 'F'
              });
              customerRefund.setValue({
                fieldId: 'account',
                value: accountId
              });
            }
            // setCreditMemoSublist(creditMemo, credit.itemList.fields);
            return +customerRefund.save();
          },
          doesnotexist: function() {
            log.error('Reference Not Found', "Externalid - " + refund.reference.externalid + ", no Credit Memo found on specified reference");
            throw nsError.create({
              message: "Reference Not Found: Externalid - " + refund.reference.externalid + ", no Credit Memo found on specified reference",
              name: 'REF_NOT_FOUND',
              notifyOff: false
            });
          },
          internalIdOnly: false
        });
      }
    };
    var unapplyPayment = function(netsuiteRecord, journalEntryId, amount) {
      // reload
      var purchaseOrder = record.load({
        type: netsuiteRecord.type,
        id: netsuiteRecord.id,
        isDynamic: true
      });
      var businessSegment = purchaseOrder.getValue('class');
      if(isEmpty(businessSegment)) purchaseOrder.setValue('class', PYMT_DFLT_BUSSEGMENT);
      var sublistId = 'apply';
      // unapply everything first
      var i = purchaseOrder.getLineCount({
        sublistId: sublistId
      }) - 1;
      while (i > 0 || i === 0) {
        purchaseOrder.selectLine({
          sublistId: sublistId,
          line: i
        });
        purchaseOrder.setCurrentSublistValue({
          sublistId: sublistId,
          fieldId: 'apply',
          value: false
        });
        purchaseOrder.commitLine({
          sublistId: sublistId
        });
        i = i - 1;
      }
      // apply
      i = purchaseOrder.getLineCount({
        sublistId: sublistId
      }) - 1;
      while (i > 0 || i === 0) {
        purchaseOrder.selectLine({
          sublistId: sublistId,
          line: i
        });
        var id = purchaseOrder.getCurrentSublistValue({
          sublistId: sublistId,
          fieldId: 'internalid'
        });
        if ("" + id === "" + journalEntryId) {
          purchaseOrder.setCurrentSublistValue({
            sublistId: sublistId,
            fieldId: 'apply',
            value: true
          });
          // purchaseOrder.setCurrentSublistValue({sublistId: sublistId, fieldId: 'amount', value: amount});
          purchaseOrder.commitLine({
            sublistId: sublistId
          });
        }
        purchaseOrder.commitLine({
          sublistId: sublistId
        });
        i = i - 1;
      }
      var poId = +purchaseOrder.save({
        enableSourcing: true,
        ignoreMandatoryFields: true
      });
      return poId;
    };
    var createJournalEntry = function(journalEntry, hays, haysEvent) {
      var refund = hays.fields;
      findExistingRecord({
        externalid: refund.reference.externalid,
        recordtype: search.Type.TRANSACTION,
        filters: [
          ['createdfrom.externalidstring', 'is', refund.reference.externalid],
          'AND',
          ['type', 'anyof', ['CustPymt']]
        ],
        exists: function(paymentRecord) {
          var subsidiaryId = +paymentRecord.getValue({
            fieldId: 'subsidiary'
          });
          var formId = getCustomFormId(subsidiaryId, 'journalentry');
          journalEntry = record.create({
            type: record.Type.JOURNAL_ENTRY,
            isDynamic: true,
            defaultValues: {
              customform: formId
            }
          });
          journalEntry.setValue({
            fieldId: 'subsidiary',
            value: subsidiaryId
          });
          // custbody_sii_orig_invoice
          var arAccntId = paymentRecord.getValue('aracct');
          var accountId = 125;
          if (+paymentRecord.getValue('account') > 0) {
            accountId = +paymentRecord.getValue('account');
          }
          var paymentClass = paymentRecord.getValue('class');
          journalEntry.setValue({
            fieldId: 'custbody_ats_type_of_journal',
            value: 1 // Chargeback
          });
          journalEntry.setValue({
            fieldId: 'tranid',
            value: "" + refund.transactionNumber
          });
          journalEntry.setValue({
            fieldId: 'externalid',
            value: "" + hays.externalid
          });
          journalEntry.setValue({
            fieldId: 'memo',
            value: "" + hays.externalid
          });
          journalEntry.setValue({
            fieldId: 'custbody_hays_event',
            value: +haysEvent.id
          });
          journalEntry.setValue({
            fieldId: 'custbody_ats_source',
            value: 2
          });
          journalEntry.setValue({
            fieldId: 'trandate',
            value: convertHaysDateToNSDate(refund.tranDate)
          });
          journalEntry.setValue({
            fieldId: 'custbody_ats_applied_to_payment',
            value: paymentRecord.id
          });
          journalEntry.setValue({
            fieldId: 'class',
            value: paymentClass
          });
          // Set the segments
          journalEntry.selectNewLine({
            sublistId: 'line'
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'account',
            value: arAccntId
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'credit',
            value: refund.total
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'entity',
            value: paymentRecord.getValue('customer')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'location',
            value: +paymentRecord.getValue('location')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'department',
            value: +paymentRecord.getValue('department')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'class',
            value: paymentClass
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_kpi_category',
            value: +paymentRecord.getValue('cseg_kpi_category')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_acq_channels',
            value: +paymentRecord.getValue('cseg_acq_channels')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_markets',
            value: +paymentRecord.getValue('cseg_markets')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_kpi_market',
            value: +paymentRecord.getValue('cseg_kpi_market')
          });
          journalEntry.commitLine({
            sublistId: 'line'
          });
          journalEntry.selectNewLine({
            sublistId: 'line'
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'account',
            value: accountId
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'debit',
            value: refund.total
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'entity',
            value: paymentRecord.getValue('customer')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'location',
            value: +paymentRecord.getValue('location')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'department',
            value: +paymentRecord.getValue('department')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'class',
            value: paymentClass
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_kpi_category',
            value: +paymentRecord.getValue('cseg_kpi_category')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_acq_channels',
            value: +paymentRecord.getValue('cseg_acq_channels')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_markets',
            value: +paymentRecord.getValue('cseg_markets')
          });
          journalEntry.setCurrentSublistValue({
            sublistId: 'line',
            fieldId: 'cseg_kpi_market',
            value: +paymentRecord.getValue('cseg_kpi_market')
          });
          journalEntry.commitLine({
            sublistId: 'line'
          });
          var journalEntryId = +journalEntry.save();
          unapplyPayment(paymentRecord, journalEntryId, refund.total);
          return journalEntryId;
        },
        doesnotexist: function() {
          throw nsError.create({
            message: "Reference Not Found: Externalid - " + refund.reference.externalid + ", no Payments found on specified reference",
            name: 'REF_NOT_FOUND',
            notifyOff: false
          });
        },
        internalIdOnly: false
      });
    };
    var findExistingRecord = function(options) {
      var netsuiteRecord = null;
      var searchFilters = [
        ['externalidstring', 'is', options.externalid]
      ];
      if (options.recordtype === 'item') {
        searchFilters = [
          ['nameinternal', 'is', options.externalid]
        ];
      }
      if (options.filters) {
        searchFilters = options.filters;
      }
      var searchCreateOptions = {
        type: options.recordtype,
        filters: searchFilters
      };
      search.create(searchCreateOptions).run().each(function(result) {
        if (options.internalIdOnly) {
          netsuiteRecord = +result.id;
        } else {
          netsuiteRecord = record.load({
            type: "" + result.recordType,
            id: +result.id,
            isDynamic: true
          });
        }
        return false;
      });
      if (netsuiteRecord !== null) {
        options.exists(netsuiteRecord);
      } else {
        options.doesnotexist();
      }
    };
    var haysRecordTypeToNSType = function(haysRecordType) {
      var nsRecordType = null;
      switch (haysRecordType) {
        case 'payment':
          nsRecordType = record.Type.CUSTOMER_PAYMENT;
          break;
        case 'creditMemo':
          nsRecordType = record.Type.CREDIT_MEMO;
          break;
        case 'refund':
          nsRecordType = record.Type.CUSTOMER_REFUND;
          break;
        default:
          nsRecordType = haysRecordType;
      }
      return "" + nsRecordType;
    };
    var getCustomFormId = function(subsidiaryId, recordType) {
      var formId = null;
      var formIds = {
        5: 55, // Brazil
        6: 63, // Italy (Main)
        7: 73, // Italy (TuoTempo)
        8: 69, // Mexico
        9: 60, // Poland
        10: 75, // Spain
        11: 67, // Turkey
        13: 64 // Italy (Gipo)
      };
      switch (recordType) {
        case 'invoice':
          // Invoice Forms
          formIds = {
            5: 188, // Brazil
            6: 159, // Italy (Main)
            7: 119, // Italy (TuoTempo)
            8: 149, // Mexico
            9: 197, // Poland
            10: 129, // Spain
            11: 109, // Turkey
            13: 169 // Italy (Gipo)
          };
          break;
        case 'customer':
          // Customer Form
          formIds = {
            5: 55, // Brazil
            6: 63, // Italy (Main)
            7: 73, // Italy (TuoTempo)
            8: 69, // Mexico
            9: 60, // Poland
            10: 75, // Spain
            11: 67, // Turkey
            13: 64 // Italy (Gipo)
          };
          break;
        case 'customerpayment':
          // Customer Payment Form
          formIds = {
            5: 193, // Brazil
            6: 164, // Italy (Main)
            7: 124, // Italy (TuoTempo)
            8: 153, // Mexico
            9: 143, // Poland
            10: 133, // Spain
            11: 114, // Turkey
            13: 174 // Italy (Gipo)
          };
          break;
        case 'creditmemo':
          // Credit Memo Form
          formIds = {
            5: 194, // Brazil
            6: 165, // Italy (Main)
            7: 125, // Italy (TuoTempo)
            8: 154, // Mexico
            9: 202, // Poland
            10: 134, // Spain
            11: 115, // Turkey
            13: 175 // Italy (Gipo)
          };
          break;
        case 'customerrefund':
          // Credit Memo Form
          formIds = {
            5: 192, // Brazil
            6: 163, // Italy (Main)
            7: 123, // Italy (TuoTempo)
            8: 152, // Mexico
            9: 146, // Poland
            10: 132, // Spain
            11: 113, // Turkey
            13: 173 // Italy (Gipo)
          };
          break;
        case 'journalentry':
          // Journal Entry Form
          formIds = {
            5: 190, // Brazil
            6: 161, // Italy (Main)
            7: 121, // Italy (TuoTempo)
            8: 156, // Mexico
            9: 201, // Poland
            10: 136, // Spain
            11: 111, // Turkey
            13: 171 // Italy (Gipo)
          };
          break;
        default:
          // not supported
          break;
      }
      /*
          switch (+subsidiaryId) {
              case 6: // italy
                  formId = 62;
                  break;
              case 11: // turkey
                  formId = 63;
                  break;
              case 5: // Brazil
                  formId = 55;
                  break;
              case 10: // Doctoralia Internet, S.L. (Spain)
                  formId = 61;
                  break;
              case 8: // Doctoralia Mexico, S.A. de C.V.
                  formId = 65;
                  break;
              case 7: // TuoTempo
                  formId = 71;
                  break;
              case 13: // gipo
                  formId = 72;
                  break;
              case 9: // ZnanyLekarz Sp. z o. o. Poland
                  formId = 64;
                  break;
              default:
                  // not supported
                  break;
          }*/
      formId = formIds[subsidiaryId] ? formIds[subsidiaryId] : null;
      return formId;
    };
    var updateHayEventStatus = function(eventId, status, error, errorList) {
      try {
        var errorCode = '';
        var errorCodeList = '';
        var errorMessageList = '';
        log.debug('errorList', errorList);
        if(!isEmpty(errorList)) {	
          if(Array.isArray(errorList)) {	
            try {	
              var parsedError = JSON.parse(errorList[0].error);	
              errorCode = parsedError.name;
              for(var i = 0; i < errorList.length; i++) {
                var err = JSON.parse(errorList[i].error);	
                var errorName = err.name;	
                var errorMessage = err.message;	
                errorCodeList = isEmpty(errorCodeList) ? errorName : errorCodeList + ';\n' + errorName;
                errorMessageList = isEmpty(errorMessageList) ? errorMessage : errorMessageList + ';\n' + errorMessage;
              }
            } catch(e) {	
              log.error('Invalid Error Code', e);	
              if(!isEmpty(errorList[0].error)) errorCode = errorList[0].error;	
            }	
          }
          if(isEmpty(errorCode)) errorCode = '';
          if(errorCode.length > 300) errorCode = errorCode.substring(0, 300);
        }
        record.submitFields({
          type: 'customrecord_hays_event',
          id: eventId,
          values: {
            custrecord_hays_event_status: status,
            custrecord_hays_event_errorcode: errorCode,
            custrecord_hays_event_error: "" + error,
            custrecord_hays_event_errorcodelist: errorCodeList,
            custrecord_hays_event_errormsglist: errorMessageList,
            custrecord_hays_event_lastprocessor: runtime.getCurrentScript().deploymentId
          }
        });
      } catch (err) {
        log.error('ERROR updateHayEventStatus', err);
      }
    };
    var STATUS;
    (function(STATUS) {
      STATUS[STATUS["PENDING"] = 1] = "PENDING";
      STATUS[STATUS["SUCCESS"] = 2] = "SUCCESS";
      STATUS[STATUS["FAILED"] = 3] = "FAILED";
      STATUS[STATUS["IGNORED"] = 4] = "IGNORED";
    })(STATUS || (STATUS = {}));
    var AddressSublist = /** @class */ (function() {
      function AddressSublist() {}
      AddressSublist.sublistId = 'addressbook';
      AddressSublist.fields = {
        DEFAULTSHIPPING: 'defaultshipping',
        DEFAULTBILLING: 'defaultbilling',
        LABEL: 'label',
        RESIDENTIAL: 'isresidential',
        ADDRESSBOOK: 'addressbookaddress'
      };
      return AddressSublist;
    }());
    exports.AddressSublist = AddressSublist;
    var ItemSublist = /** @class */ (function() {
      function ItemSublist() {}
      ItemSublist.sublistId = 'item';
      ItemSublist.fields = {
        ITEM: 'item',
        QUANTITY: 'quantity',
        RATE: 'rate',
        AMOUNT: 'amount',
        TAXCODE: 'taxcode',
        DESCRIPTION: 'description',
        CLASS: 'class',
        REVRECSTARTDATE: 'custcol_ats_revrec_startdate',
        REVRECENDDATE: 'custcol_ats_revrec_enddate',
        LINEID: 'custcol_hays_line_id'
      };
      return ItemSublist;
    }());
    exports.ItemSublist = ItemSublist;
    var findSubsidiary = function(subsidiaryname) {
      var subsidiaryId = -1;
      search.create({
        type: 'subsidiary',
        filters: [
          ['name', 'contains', subsidiaryname]
        ],
      }).run().each(function(result) {
        subsidiaryId = +result.id;
        return false;
      });
      return subsidiaryId;
    };
    var findSalesRep = function(email) {
      var employeeId = null;
      try {
        search.create({
          type: search.Type.EMPLOYEE,
          filters: [
            ['email', 'is', email],
            'AND',
            ['salesrep', 'is', 'T'],
            'AND',
            ['isinactive', 'is', 'F']
          ]
        }).run().each(function(result) {
          employeeId = +result.id;
          return false;
        });
      } catch (e) {}
      return employeeId;
    };
    var findPaymentMethod = function(paymentMethod, provider_name) {
      var paymentMethodId = null;
      var filters = [
        ['name', 'contains', "" + paymentMethod.replace(/_/g, ' ')]
      ];
      if(provider_name == 'd_local') {
        filters.push('AND');
        filters.push(['name', 'contains', 'dLocal']);
      }
      else if (provider_name && provider_name.length > 0) {
        filters.push('AND');
        var gatewayText = "" + provider_name.split('_')[0];
        filters.push(['name', 'contains', gatewayText]);
      }
      search.create({
        type: 'paymentmethod',
        filters: filters,
      }).run().each(function(result) {
        paymentMethodId = +result.id;
        return false;
      });
      return paymentMethodId;
    };
    var findPaymentGateway = function(provider_name, subsidiaryId) {
      var gatewayText = '';
      if(provider_name == 'd_local') {
        gatewayText = 'dLocal';
      } else {
        gatewayText = ("" + provider_name.split('_')[0]).trim();
      }
      
      var paymentGatewayId = null;
      var filters = [
        ['name', 'contains', gatewayText]
      ];
      if (gatewayText.toLowerCase() === 'payu') {
        if (subsidiaryId === SUBSIDIARIES.TURKEY) {
          filters.push('AND');
          filters.push(['name', 'contains', "TR"]);
        } else if (subsidiaryId === SUBSIDIARIES.POLAND) {
          filters.push('AND');
          filters.push(['name', 'contains', "PL"]);
        }
      }
      search.create({
        type: 'customlist_ats_payment_gateway_list',
        filters: filters,
      }).run().each(function(result) {
        paymentGatewayId = +result.id;
        return false;
      });
      return paymentGatewayId;
    };
    var findAccountId = function(subsidiaryId, countryId, paymentGatewayId, paymentMethodId, payu_3ds) {
      var accountId = null;
      // let is3dsec: boolean = false;
      var filters = [
        ['custrecord_hays_account_subsidiary', 'anyof', subsidiaryId]
      ];
      if (+countryId > 0) {
        filters.push('AND', ['custrecord_hays_account_country', 'anyof', '@NONE@', countryId]);
      } else {
        filters.push('AND', ['custrecord_hays_account_country', 'anyof', '@NONE@']);
      }
      if (+paymentGatewayId > 0) {
        filters.push('AND', ['custrecord_hays_account_payment_gateway', 'anyof', '@NONE@', paymentGatewayId]);
      } else {
        filters.push('AND', ['custrecord_hays_account_payment_gateway', 'anyof', '@NONE@']);
      }
      if (+paymentMethodId > 0) {
        filters.push('AND', ['custrecord_hays_account_paymentmethod', 'anyof', '@NONE@', paymentMethodId]);
      }
      if (payu_3ds) {
        filters.push('AND', ['custrecordhays_account_3dsec_enabled', 'is', 'T']);
      } else {
        filters.push('AND', ['custrecordhays_account_3dsec_enabled', 'is', 'F']);
      }
      search.create({
        type: 'customrecord_hays_account_mapping',
        filters: filters,
        columns: [
          search.createColumn({
            name: 'internalid',
            join: 'CUSTRECORD_HAYS_ACCOUNT_ACCOUNT',
            label: 'Internal ID'
          }),
          search.createColumn({
            name: 'custrecordhays_account_3dsec_enabled',
            label: '3D Sec Enabled'
          })
        ]
      }).run().each(function(result) {
        accountId = +result.getValue(search.createColumn({
          name: 'internalid',
          join: 'CUSTRECORD_HAYS_ACCOUNT_ACCOUNT',
          label: 'Internal ID'
        }));
        return false;
      });
      return {
        account: accountId
      };
    };
    var findARAccountId = function(subsidiaryId, countryId) {
      var accountId = null;
      var prefixId = null;
      var filters = [
        ['custrecord_hays_ar_subsidiary', 'anyof', subsidiaryId]
      ];
      if (countryId) {
        filters.push('AND', ['custrecord_hays_ar_country', 'anyof', countryId]);
      }
      search.create({
        type: 'customrecord_hays_ar_mapping',
        filters: filters,
        columns: [
          search.createColumn({
            name: 'internalid',
            label: 'Internal ID'
          }),
          search.createColumn({
            name: 'custrecord_hays_ar_account',
            label: 'AR Account'
          }),
          search.createColumn({
            name: 'custrecord_hays_ar_prefix',
            label: 'Prefix Invoice Number'
          })
        ]
      }).run().each(function(result) {
        accountId = +result.getValue('custrecord_hays_ar_account');
        prefixId = "" + result.getValue('custrecord_hays_ar_prefix');
        return false;
      });
      return {
        araccount: accountId,
        prefix: prefixId
      };
    };
  
    function getKPIMarket(marketId) {
      var kpiMarketId = '';
      var searchObj = search.create({ type: 'customrecord_cseg_kpi_market' });
      var columns = [];
      columns.push(search.createColumn({
        name: 'name',
        label: 'Name'
      }));
      searchObj.columns = columns;
      var filters = [];
      filters.push(search.createFilter({
        name: 'isinactive',
        operator: search.Operator.IS,
        values: 'F'
      }));
      filters.push(search.createFilter({
        name: 'cseg_kpi_market_filterby_cseg_markets',
        operator: search.Operator.ANYOF,
        values: marketId
      }));
      searchObj.filters = filters;
      var result = searchObj.run().getRange(0, 1);
      if(!isEmpty(result)) kpiMarketId = result[0].id;
  
      return kpiMarketId;
    }
  
    function isEmpty(stValue) {
      return ((stValue === '' || stValue == null || stValue == undefined) || (stValue.constructor === Array && stValue.length == 0) || (stValue.constructor === Object && (function(
        v) {
        for (var k in v)
          return false;
        return true;
      })(stValue)));
    }
  
    // Alternative for Array.prototype.includes (Function was introduced in ES7)
    function isInArray(value, array) {
      return array.indexOf(value) > -1;
    }
  
    function getTransactionInternalId(externalId) {
      var searchObj = search.create({ type: 'transaction' });
      var columns = [];
      columns.push(search.createColumn({
        name: 'transactionname',
        label: 'Transaction Name'
      }));
      searchObj.columns = columns;
      var filters = [];
      filters.push(search.createFilter({
        name: 'externalidstring',
        operator: 'is',
        values: externalId
      }));
      filters.push(search.createFilter({
        name: 'mainline',
        operator: 'is',
        values: 'T'
      }));
      searchObj.filters = filters;
      results = searchObj.run().getRange(0, 1);
      if(isEmpty(results)) throw nsError.create({
        message: 'No Invoice found with External ID of ' + externalId,
        name: 'NO_INVOICE_FOUND',
        notifyOff: true
      });
  
      return results[0].id;
    }
  
    function getCreditMemoByInvoice(invoiceId) {
      var creditMemoId = '';
  
      var searchObj = search.create({
        type: 'creditmemo'
      });
      var columns = [];
      columns.push(search.createColumn({
        name: 'internalid',
        label: 'Internal ID'
      }));
      searchObj.columns = columns;
      var filters = [];
      filters.push(search.createFilter({
        name: 'mainline',
        operator: 'is',
        values: 'T'
      }));
      filters.push(search.createFilter({
        name: 'type',
        operator: 'anyof',
        values: 'CustCred'
      }));
      filters.push(search.createFilter({
        name: 'createdfrom',
        operator: 'anyof',
        values: invoiceId
      }));
      searchObj.filters = filters;
      results = searchObj.run().getRange(0, 1);
      if(!isEmpty(results)) creditMemoId = results[0].id;
  
      return creditMemoId;
    }
  
    function getPaisesId(countryName){
      var paisesId = '';
      var searchObj = search.create({ type: 'customrecord_xpaises'});
      var columns = [];
      columns.push(search.createColumn({
        name: 'internalid',
        label: 'Internal ID'
      }));
      searchObj.columns = columns;
      var filters = [];
      filters.push(search.createFilter({
        name: 'isinactive',
        operator: 'is',
        values: 'F'
      }));
      filters.push(search.createFilter({
        name: 'formulatext',
        formula: '{custrecord_xpaisespaisns}',
        operator: 'is',
        values: countryName
      }));
      searchObj.filters = filters;
  
      results = searchObj.run().getRange(0, 1);
      if(!isEmpty(results)) paisesId = results[0].id;
  
      return paisesId;
    }
  
  
  });