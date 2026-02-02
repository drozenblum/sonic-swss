#include "monitTXstatusorch.h"

MonitTXStatusOrch::MonitTXStatusOrch(DBConnector *configDb)
    : Orch(configDb, CFG_MONIT_TX_CONFIG_TABLE_NAME)
{
    SWSS_LOG_ENTER();
    SWSS_LOG_NOTICE("MonitTXStatusOrch constructor started");
    
    std::shared_ptr<swss::DBConnector> countersDb = std::make_shared<DBConnector>("COUNTERS_DB", 0);
    m_countersTable = std::make_shared<swss::Table>(countersDb.get(), COUNTERS_TABLE);
    
    std::shared_ptr<swss::DBConnector> stateDb = std::make_shared<DBConnector>("STATE_DB", 0);
    m_stateTable = std::make_shared<swss::Table>(stateDb.get(), STATE_MONIT_TX_STATUS_TABLE_NAME);

    m_configTable = std::unique_ptr<swss::Table>(new swss::Table(configDb, CFG_MONIT_TX_CONFIG_TABLE_NAME));

    loadConfig();

    m_timer = new SelectableTimer(timespec { .tv_sec = static_cast<time_t>(m_timeInterval), .tv_nsec = 0 });
    auto executor = new ExecutableTimer(m_timer, this, "MONIT_TX_STATUS_TIMER");
    Orch::addExecutor(executor);
    m_timer->start();

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Timer executor registered");

    SWSS_LOG_NOTICE("MonitTXStatusOrch initialized with interval=%lu sec, threshold=%lu",
                    m_timeInterval, m_TxErrorThreshold);
}

MonitTXStatusOrch::~MonitTXStatusOrch(void)
{
    SWSS_LOG_ENTER();
}

void MonitTXStatusOrch::updateAvailablePorts()
{
    SWSS_LOG_ENTER();
    SWSS_LOG_NOTICE("MonitTXStatusOrch: Updating available ports");

    if (!gPortsOrch)
    {
        SWSS_LOG_WARN("MonitTXStatusOrch: PortsOrch not available yet");
        return;
    }

    // Get all ports from PortsOrch
    for (const auto &pair : gPortsOrch->getAllPorts())
    {
        const Port &port = pair.second;
        // Only monitor physical ports
        if (port.m_type == Port::PHY)
        {
            addPort(port);
        }
    }

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Initialized monitoring for %zu ports", m_ports.size());
}

void MonitTXStatusOrch::addPort(const Port &port)
{
    SWSS_LOG_ENTER();

    if (m_ports.find(port.m_alias) == m_ports.end())
    {
        m_ports[port.m_alias] = port;
        m_portTxErrors[port.m_port_id] = getPortTxErrors(port);
        
        SWSS_LOG_INFO("MonitTXStatusOrch: Added port %s for TX error monitoring", port.m_alias.c_str());
    }
}

void MonitTXStatusOrch::loadConfig()
{
    SWSS_LOG_ENTER();

    vector<FieldValueTuple> fieldValues;

    // Load time_interval from MONIT_TX_CONFIG|time_interval
    if (m_configTable->get(CFG_MONIT_TX_TIME_INTERVAL_KEY, fieldValues))
    {
        for (const auto &fv : fieldValues)
        {
            if (fvField(fv) == CFG_MONIT_TX_VALUE_FIELD)
            {
                try
                {
                    m_timeInterval = stoull(fvValue(fv));
                    SWSS_LOG_INFO("MonitTXStatusOrch: Loaded time_interval: %lu", m_timeInterval);
                }
                catch (const std::exception &e)
                {
                    SWSS_LOG_ERROR("MonitTXStatusOrch: Invalid time_interval value: %s, using default", fvValue(fv).c_str());
                    m_timeInterval = DEFAULT_POLL_INTERVAL_SEC;
                }
                break;
            }
        }
    }
    else
    {
        SWSS_LOG_NOTICE("MonitTXStatusOrch: No config found for %s|%s, using default: %d", 
                        CFG_MONIT_TX_CONFIG_TABLE_NAME, CFG_MONIT_TX_TIME_INTERVAL_KEY, DEFAULT_POLL_INTERVAL_SEC);
    }

    fieldValues.clear();

    // Load error_threshold from MONIT_TX_CONFIG|error_threshold
    if (m_configTable->get(CFG_MONIT_TX_ERROR_THRESHOLD_KEY, fieldValues))
    {
        for (const auto &fv : fieldValues)
        {
            if (fvField(fv) == CFG_MONIT_TX_VALUE_FIELD)
            {
                try
                {
                    m_TxErrorThreshold = stoull(fvValue(fv));
                    SWSS_LOG_INFO("MonitTXStatusOrch: Loaded error_threshold: %lu", m_TxErrorThreshold);
                }
                catch (const std::exception &e)
                {
                    SWSS_LOG_ERROR("MonitTXStatusOrch: Invalid error_threshold value: %s, using default", fvValue(fv).c_str());
                    m_TxErrorThreshold = DEFAULT_ERROR_THRESHOLD;
                }
                break;
            }
        }
    }
    else
    {
        SWSS_LOG_NOTICE("MonitTXStatusOrch: No config found for %s|%s, using default: %d", 
                        CFG_MONIT_TX_CONFIG_TABLE_NAME, CFG_MONIT_TX_ERROR_THRESHOLD_KEY, DEFAULT_ERROR_THRESHOLD);
    }
}

/*
 * Handles config_db updates for time interval and error threshold.
 * Config structure: two keys in MONIT_TX_CONFIG table
 *   - MONIT_TX_CONFIG|time_interval with field "value"
 *   - MONIT_TX_CONFIG|error_threshold with field "value"
 */
void MonitTXStatusOrch::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch consumer task started");

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        const string &key = kfvKey(it->second);
        const string &op = kfvOp(it->second);
        const vector<FieldValueTuple> &fvs = kfvFieldsValues(it->second);

        if (op == SET_COMMAND)
        {
            if (key == CFG_MONIT_TX_TIME_INTERVAL_KEY)
            {
                for (const auto &fv : fvs)
                {
                    if (fvField(fv) == CFG_MONIT_TX_VALUE_FIELD)
                    {
                        try
                        {
                            m_timeInterval = stoull(fvValue(fv));
                            SWSS_LOG_NOTICE("MonitTXStatusOrch: Updated time_interval to %lu", m_timeInterval);

                            m_timer->setInterval(timespec { .tv_sec = static_cast<time_t>(m_timeInterval), .tv_nsec = 0 });
                            m_timer->reset();
                            resetTXErrorCounters();
                        }
                        catch (const std::exception &e)
                        {
                            SWSS_LOG_ERROR("MonitTXStatusOrch: Invalid time_interval value: %s", fvValue(fv).c_str());
                        }
                        break;
                    }
                }
            }
            else if (key == CFG_MONIT_TX_ERROR_THRESHOLD_KEY)
            {
                for (const auto &fv : fvs)
                {
                    if (fvField(fv) == CFG_MONIT_TX_VALUE_FIELD)
                    {
                        try
                        {
                            m_TxErrorThreshold = stoull(fvValue(fv));
                            SWSS_LOG_NOTICE("MonitTXStatusOrch: Updated error_threshold to %lu", m_TxErrorThreshold);
                        }
                        catch (const std::exception &e)
                        {
                            SWSS_LOG_ERROR("MonitTXStatusOrch: Invalid error_threshold value: %s", fvValue(fv).c_str());
                        }
                        break;
                    }
                }
            }
        }
        it = consumer.m_toSync.erase(it);
    }
}

void MonitTXStatusOrch::doTask(SelectableTimer &timer)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch timer fired, checking ports");

    if(!gPortsOrch)
    {
        SWSS_LOG_WARN("MonitTXStatusOrch: PortsOrch not available yet");
        return;
    }

    updateAvailablePorts();

    if(m_ports.empty())
    {
        SWSS_LOG_NOTICE("MonitTXStatusOrch: No ports found for monitoring (gPortsOrch=%s)", 
                        gPortsOrch ? "available" : "NULL");
        return;
    }

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Checking TX status for %zu ports", m_ports.size());

    for (const auto &portPair : m_ports)
    {
        updatePortTxStatus(portPair.second);
    }
}

void MonitTXStatusOrch::updatePortTxStatus(const Port &port)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Updating TX status for port %s", port.m_alias.c_str());

    uint64_t currentTxErrors = getPortTxErrors(port);
    uint64_t previousTxErrors = m_portTxErrors[port.m_port_id];

    
    uint64_t txErrorsDelta = 0;
    
    if(currentTxErrors == NA_VALUE || previousTxErrors == NA_VALUE){
        txErrorsDelta = NA_VALUE;
    }
    else if (currentTxErrors >= previousTxErrors)
    {
        txErrorsDelta = currentTxErrors - previousTxErrors;
    }
    else
    {
        SWSS_LOG_DEBUG("MonitTXStatusOrch: End counter value is less than start counter value for port %s", port.m_alias.c_str());
    }

    m_portTxErrors[port.m_port_id] = currentTxErrors;

    if(txErrorsDelta == NA_VALUE){
        updateStateDb(port, NA_STATUS);
        SWSS_LOG_WARN("MonitTXStatusOrch: Port %s TX status is N/A", port.m_alias.c_str());
        return;
    }
    else if (txErrorsDelta > m_TxErrorThreshold)
    {
        updateStateDb(port, MONIT_TX_STATE_ERROR);
        SWSS_LOG_WARN("MonitTXStatusOrch: Port %s TX errors (%lu) exceeded threshold (%lu)",
                      port.m_alias.c_str(), txErrorsDelta, m_TxErrorThreshold);
    }
    else
    {
        updateStateDb(port, MONIT_TX_STATE_OK);
        SWSS_LOG_DEBUG("MonitTXStatusOrch: Port %s TX status OK (errors: %lu)", port.m_alias.c_str(), txErrorsDelta);
    }
}

uint64_t MonitTXStatusOrch::getPortTxErrors(const Port &port)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Getting TX errors for port %s", port.m_alias.c_str());

    sai_object_id_t portId = port.m_port_id;
    string key = sai_serialize_object_id(portId);
    
    vector<FieldValueTuple> fieldValues;
    if (!m_countersTable->get(key, fieldValues))
    {
        SWSS_LOG_DEBUG("MonitTXStatusOrch: No counters found for port %s (OID: %s)", 
                       port.m_alias.c_str(), key.c_str());
        return NA_VALUE;
    }

    for (const auto &fv : fieldValues)
    {
        if (fvField(fv) == SAI_PORT_TX_ERRORS_COUNTER)
        {
            try
            {
                return stoull(fvValue(fv));
            }
            catch (const std::exception &e)
            {
                SWSS_LOG_ERROR("MonitTXStatusOrch: Failed to parse TX errors for port %s: %s",
                               port.m_alias.c_str(), fvValue(fv).c_str());
                return NA_VALUE;
            }
        }
    }

    SWSS_LOG_DEBUG("MonitTXStatusOrch: SAI_PORT_STAT_IF_OUT_ERRORS not found for port %s", port.m_alias.c_str());
    return NA_VALUE;
}

void MonitTXStatusOrch::updateStateDb(const Port &port, const std::string &status)
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Updating state DB for port %s with status %s", port.m_alias.c_str(), status.c_str());

    vector<FieldValueTuple> fieldValues;
    fieldValues.emplace_back(STATE_COL_NAME_TX_STATUS, status);

    m_stateTable->set(port.m_alias, fieldValues);
}

void MonitTXStatusOrch::resetTXErrorCounters()
{
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("MonitTXStatusOrch: Resetting TX errors for all ports");

    for (const auto &portPair : m_ports)
    {
        m_portTxErrors[portPair.second.m_port_id] = getPortTxErrors(portPair.second);
        updateStateDb(portPair.second, MONIT_TX_STATE_RESET);
    }
}
