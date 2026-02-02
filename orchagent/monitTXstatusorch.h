#ifndef MONITTXSTATUSORCH_H
#define MONITTXSTATUSORCH_H

#include "orch.h"
#include "port.h"
#include "timer.h"
#include "selectabletimer.h"
#include "portsorch.h"
#include "logger.h"
#include "sai_serialize.h"
#include "schema.h"
#include <map>
#include <memory>

extern "C" {
#include "sai.h"
}
extern PortsOrch *gPortsOrch;


// Default configuration values
#define DEFAULT_POLL_INTERVAL_SEC   60      
#define DEFAULT_ERROR_THRESHOLD     100

// State values
#define MONIT_TX_STATE_ERROR "NOT OK"
#define MONIT_TX_STATE_OK "OK"
#define MONIT_TX_STATE_RESET "RESET"
#define NA_STATUS "N/A"
#define NA_VALUE UINT64_MAX

#define CFG_MONIT_TX_TIME_INTERVAL_KEY      "time_interval"
#define CFG_MONIT_TX_ERROR_THRESHOLD_KEY    "error_threshold"

#define CFG_MONIT_TX_VALUE_FIELD            "value"

// State field names
#define STATE_COL_NAME_TX_STATUS "tx_status"

#define SAI_PORT_TX_ERRORS_COUNTER "SAI_PORT_STAT_IF_OUT_ERRORS"

using namespace swss;
using namespace std;

class MonitTXStatusOrch : public Orch
{
public:
    MonitTXStatusOrch(DBConnector *configDb);
    ~MonitTXStatusOrch(void);

    virtual void doTask(SelectableTimer &timer) override;
    virtual void doTask(Consumer &consumer) override;

private:
    std::unordered_map<std::string, Port> m_ports;
    std::unordered_map<sai_object_id_t, uint64_t> m_portTxErrors;
    uint64_t m_TxErrorThreshold = DEFAULT_ERROR_THRESHOLD;
    
    std::shared_ptr<swss::Table> m_countersTable = nullptr;
    std::shared_ptr<swss::Table> m_stateTable = nullptr;
    std::unique_ptr<swss::Table> m_configTable = nullptr;

    swss::SelectableTimer *m_timer = nullptr;
    uint64_t m_timeInterval = DEFAULT_POLL_INTERVAL_SEC;

    void updatePortTxStatus(const Port &port);
    uint64_t getPortTxErrors(const Port &port);
    void updateStateDb(const Port &port, const std::string &status);
    void resetTXErrorCounters();
    void loadConfig();
    void updateAvailablePorts();
    void addPortToMonitoring(const Port &port);
};

#endif
