/**
 * @NApiVersion 2.1
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 */
 define(['N/https', 'N/search', 'N/record', 'N/runtime'], function (https, search, record, runtime) {

  /**
   * Function definition to be triggered before record is loaded.
   *
   * @param {Object} scriptContext
   * @param {Record} scriptContext.newRecord - New record
   * @param {Record} scriptContext.oldRecord - Old record
   * @param {string} scriptContext.type - Trigger type
   * @Since 2015.2
   */
  function afterSubmit(scriptContext) {
    if(scriptContext.type != scriptContext.UserEventType.CREATE && scriptContext.type != scriptContext.UserEventType.EDIT) return;

    const isForHAYSIntegration = scriptContext.newRecord.getValue('custbody_ats_for_hays_integration');
    if(!isForHAYSIntegration) return;

    try {
      // Get HAYS Credential Record for Authentication
      const subsidiary = scriptContext.newRecord.getValue('subsidiary');
      const credentials = getHaysCredential(subsidiary);
      if(isEmpty(credentials.serverURL)) return;

      // Build Payload
      const payload_list = [];
      const applyLineCount = scriptContext.newRecord.getLineCount('apply');
      for(let i = 0; i < applyLineCount; i++) {
        const isApplied = scriptContext.newRecord.getSublistValue({
          sublistId: 'apply',
          fieldId: 'apply',
          line: i
        });
        if(!isApplied) continue;
        let paymentAmount = parseFloat(scriptContext.newRecord.getSublistValue({
          sublistId: 'apply',
          fieldId: 'amount',
          line: i
        }));
        if(isNaN(paymentAmount)) paymentAmount = 0;
        const internalid = scriptContext.newRecord.getSublistValue({
          sublistId: 'apply',
          fieldId: 'internalid',
          line: i
        });
        const referenceNum = scriptContext.newRecord.getSublistValue({
          sublistId: 'apply',
          fieldId: 'refnum',
          line: i
        });
        const tran_fields = search.lookupFields({
          type: search.Type.TRANSACTION,
          id: internalid,
          columns: ['externalid', 'otherrefnum']
        });
        const tran_externalId = isEmpty(tran_fields.externalid) ? tran_fields.otherrefnum : tran_fields.externalid[0].value;
        const payload = {
          //invoice_id: 'pl_CrmInvoice_535801', //Test data for sending. Comment this then uncomment the other line for the legit code.
          invoice_id: tran_externalId,
          amount: paymentAmount,
          currency: getCurrencyISOCode(scriptContext.newRecord.getValue('currency')),
          net_suite_refund_id: scriptContext.newRecord.id.toString(),
          net_suite_payment_id: scriptContext.newRecord.getSublistValue({ sublistId: 'apply', fieldId: 'internalid', line: i}),
          environment: runtime.envType
        };

        let errorMessage = '';
        if(isEmpty(payload.invoice_id)) errorMessage = `Refund not sent. No External ID found on ${referenceNum}.`
        else if(isEmpty(payload.amount)) errorMessage = `Refund not sent. No Payment Amount found on ${referenceNum}.`
        else if(isEmpty(payload.currency)) errorMessage = `Refund not sent. No Currency found on this Refund.`
        else if(isEmpty(payload.net_suite_refund_id)) errorMessage = `Refund not sent. No Internal ID found on this Refund.`
        else if(isEmpty(payload.net_suite_payment_id)) errorMessage = `Refund not sent. No Payment Internal ID found on this Refund.`
        if(!isEmpty(errorMessage)) {
          record.submitFields({
            type: record.Type.CUSTOMER_REFUND,
            id: scriptContext.newRecord.id,
            values: {
              custbody_ats_haysresponse: errorMessage
            },
            options: {
              enableSourcing: false,
              ignoreMandatoryFields : true
            }
          });
          return;
        }

        payload_list.push(payload);
      }

      // Authentication
      const authHeaders = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cache-Control': 'no-cache'
      };
      const token = getToken(credentials.ssoURL, authHeaders, credentials);
      log.debug('Token', token);
      
      // Send Payloads to Server
      log.debug('Payload', payload_list);
      const response_list = [];
      const responseBody_list = [];
      let result = '';

      const isSentToHAYS = scriptContext.newRecord.getValue('custbody_ats_issenttohays');
      if(!isSentToHAYS) {
        payload_list.forEach(payload => {
          const response = https.post({
            body: JSON.stringify(payload),
            url: credentials.serverURL,
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token.access_token}`
            }
          });
          const dateSent = new Date();
          const dateSentString = dateSent.toString();
          const dateSentISO = dateSent.toISOString();
          response_list.push(response);
          responseBody_list.push(response.body);
          if(response.code >= 200 && response.code <= 299) {
            result = isEmpty(result) ? `Success` : `${result}, Success`;
            payload.sendStatus = 'Success';
          }
          else {
            result = isEmpty(result) ? `Failed` : `${result}, Failed`;
            payload.sendStatus = 'Failed';
          }
          payload.dateSentString = dateSentString;
          payload.dateSentISO = dateSentISO;
        });
      }
      else {
        const existingPayload_str = scriptContext.newRecord.getValue('custbody_ats_nstohayspayload');
        const existingPayload_list = isEmpty(existingPayload_str) ? [] : JSON.parse(existingPayload_str);
        let doNotSend = false;
        payload_list.forEach(payload => {
          existingPayload_list.forEach(existingPayload => {
            if(existingPayload.sendStatus != 'Success') return;
            if(doNotSend == true) return;
            if(existingPayload.invoice_id == payload.invoice_id && existingPayload.amount == payload.amount) doNotSend = true;
          });
          if(doNotSend == true)  {
            result = isEmpty(result) ? `Success` : `${result}, Success`;
            payload.sendStatus = 'Success';
            return;
          }
          const response = https.post({
            body: JSON.stringify(payload),
            url: credentials.serverURL,
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${token.access_token}`
            }
          });
          const dateSent = new Date();
          const dateSentString = dateSent.toString();
          const dateSentISO = dateSent.toISOString();
          response_list.push(response);
          responseBody_list.push(response.body);
          if(response.code >= 200 && response.code <= 299) {
            result = isEmpty(result) ? `Success` : `${result}, Success`;
            payload.sendStatus = 'Success';
          }
          else {
            result = isEmpty(result) ? `Failed` : `${result}, Failed`;
            payload.sendStatus = 'Failed';
          }
          payload.dateSentString = dateSentString;
          payload.dateSentISO = dateSentISO;
        });
      }
      
      log.debug('Response List', response_list);
      record.submitFields({
        type: record.Type.CUSTOMER_REFUND,
        id: scriptContext.newRecord.id,
        values: {
          custbody_ats_haysresponse: responseBody_list,
          custbody_ats_issenttohays: true,
          custbody_ats_nstohayspayload: JSON.stringify(payload_list),
          custbody_ats_nstohaysresult: result
        },
        options: {
          enableSourcing: false,
          ignoreMandatoryFields : true
        }
      });
    } catch(e) {
      log.error(`Error Encountered on Customer Refund ID ${scriptContext.newRecor.id}`, JSON.stringify(e));
    }
  }

  function getHaysCredential(subsidiaryId) {
    const credentials = {
      ssoURL: '',
      clientId: '',
      clientSecret: '',
      scope: '',
      grantType: '',
      serverURL: ''
    }

    const searchObj = search.create({ type: 'customrecord_hays_credential' });
    const columns = [];
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_ssourl',
      label: 'SSO URL'
    }));
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_clientid',
      label: 'Client ID'
    }));
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_clientsecret',
      label: 'Client Secret'
    }));
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_scope',
      label: 'Scope'
    }));
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_granttype',
      label: 'Grant Type'
    }));
    columns.push(search.createColumn({
      name: 'custrecord_hays_credential_serverurl',
      label: 'Server URL'
    }));
    searchObj.columns = columns;
    const filters = [];
    filters.push(search.createFilter({
      name: 'custrecord_hays_credential_subsidiary',
      operator: search.Operator.ANYOF,
      values: subsidiaryId
    }));
    filters.push(search.createFilter({
      name: 'isinactive',
      operator: search.Operator.IS,
      values: 'F'
    }));
    searchObj.filters = filters;
    searchResults = searchObj.run().getRange(0, 1);
    if(isEmpty(searchResults)) return credentials;

    credentials.ssoURL = searchResults[0].getValue('custrecord_hays_credential_ssourl');
    credentials.clientId = searchResults[0].getValue('custrecord_hays_credential_clientid');
    credentials.clientSecret = searchResults[0].getValue('custrecord_hays_credential_clientsecret');
    credentials.scope = searchResults[0].getValue('custrecord_hays_credential_scope');
    credentials.grantType = searchResults[0].getValue('custrecord_hays_credential_granttype');
    credentials.serverURL = searchResults[0].getValue('custrecord_hays_credential_serverurl');

    return credentials;
  }

  function getToken(url, headers, credentials) {
    const payload = `client_id=${credentials.clientId}&client_secret=${credentials.clientSecret}&scope=${credentials.scope}&grant_type=${credentials.grantType}`
    const response = https.post({
      body: payload,
      url: url,
      headers: headers
    });
    const responseBody = JSON.parse(response.body);
  
    return responseBody;
  }

  function getCurrencyISOCode(internalid) {
    let currencyISOCode = '';
    if(isEmpty(internalid)) return currencyISOCode;
    const currency_search = search.create({ type: 'currency' });
    const columns = [];
    columns.push(search.createColumn({
      name: 'symbol',
      label: 'Symbol'
    }));
    currency_search.columns = columns;
    const filters = [];
    filters.push(search.createFilter({
      name: 'internalid',
      operator: search.Operator.ANYOF,
      values: internalid
    }));
    currency_search.filters = filters;
    const currency_results = currency_search.run().getRange(0, 1);
    if(!isEmpty(currency_results)) currencyISOCode = currency_results[0].getValue('symbol');
    return currencyISOCode;
  }

  function isEmpty(stValue) {
    return ((stValue === '' || stValue == null || stValue == undefined) || (stValue.constructor === Array && stValue.length == 0) || (stValue.constructor === Object && (function(
      v) {
      for (let k in v)
        return false;
      return true;
    })(stValue)));
  }

  return {
    afterSubmit: afterSubmit
  };

});
