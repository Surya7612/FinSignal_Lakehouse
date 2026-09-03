#include "finsignal/event_engine.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

PYBIND11_MODULE(finsignal_engine, m) {
  m.doc() = "FinSignal deterministic C++20 position reconstruction engine";

  py::class_<finsignal::Trade>(m, "Trade")
      .def(py::init<>())
      .def_readwrite("account_id", &finsignal::Trade::account_id)
      .def_readwrite("security_id", &finsignal::Trade::security_id)
      .def_readwrite("trade_date", &finsignal::Trade::trade_date)
      .def_readwrite("side", &finsignal::Trade::side)
      .def_readwrite("quantity", &finsignal::Trade::quantity);

  py::class_<finsignal::StartingPosition>(m, "StartingPosition")
      .def(py::init<>())
      .def_readwrite("account_id", &finsignal::StartingPosition::account_id)
      .def_readwrite("security_id", &finsignal::StartingPosition::security_id)
      .def_readwrite("position_date", &finsignal::StartingPosition::position_date)
      .def_readwrite("quantity", &finsignal::StartingPosition::quantity);

  py::class_<finsignal::ReportedPosition>(m, "ReportedPosition")
      .def(py::init<>())
      .def_readwrite("account_id", &finsignal::ReportedPosition::account_id)
      .def_readwrite("security_id", &finsignal::ReportedPosition::security_id)
      .def_readwrite("position_date", &finsignal::ReportedPosition::position_date)
      .def_readwrite("reported_quantity", &finsignal::ReportedPosition::reported_quantity);

  py::class_<finsignal::QualityFlag>(m, "QualityFlag")
      .def(py::init<>())
      .def_readwrite("account_id", &finsignal::QualityFlag::account_id)
      .def_readwrite("security_id", &finsignal::QualityFlag::security_id)
      .def_readwrite("flag_date", &finsignal::QualityFlag::flag_date)
      .def_readwrite("flag_type", &finsignal::QualityFlag::flag_type);

  py::class_<finsignal::PositionRow>(m, "PositionRow")
      .def(py::init<>())
      .def_readwrite("account_id", &finsignal::PositionRow::account_id)
      .def_readwrite("security_id", &finsignal::PositionRow::security_id)
      .def_readwrite("position_date", &finsignal::PositionRow::position_date)
      .def_readwrite("starting_quantity", &finsignal::PositionRow::starting_quantity)
      .def_readwrite("cumulative_buy_quantity", &finsignal::PositionRow::cumulative_buy_quantity)
      .def_readwrite("cumulative_sell_quantity", &finsignal::PositionRow::cumulative_sell_quantity)
      .def_readwrite("expected_position", &finsignal::PositionRow::expected_position)
      .def_readwrite("reported_position", &finsignal::PositionRow::reported_position)
      .def_readwrite("position_difference", &finsignal::PositionRow::position_difference)
      .def_readwrite("reconciliation_status", &finsignal::PositionRow::reconciliation_status)
      .def_readwrite("break_reason_code", &finsignal::PositionRow::break_reason_code);

  py::class_<finsignal::EventEngine>(m, "EventEngine")
      .def(py::init<>())
      .def("set_valid_securities", &finsignal::EventEngine::set_valid_securities)
      .def("add_trade", &finsignal::EventEngine::add_trade)
      .def("add_starting_position", &finsignal::EventEngine::add_starting_position)
      .def("add_reported_position", &finsignal::EventEngine::add_reported_position)
      .def("add_quality_flag", &finsignal::EventEngine::add_quality_flag)
      .def("clear", &finsignal::EventEngine::clear)
      .def("reconstruct", &finsignal::EventEngine::reconstruct);
}
