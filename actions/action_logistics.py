from datetime import datetime
from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker
from rasa_sdk.events import SlotSet
from rasa_sdk.executor import CollectingDispatcher
from sqlalchemy.orm import joinedload

from actions.db import SessionLocal
from actions.db_table_class import LogisticsCompany, OrderInfo, Logistics, LogisticsComplaint, LogisticsComplaintsRecord


class ActionLogisticsCompanys(Action):

    def name(self) -> Text:
        return "action_get_logistics_companys"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        # 查询mysql中logistics_company表中快递厂商的名字，返回结果给用户
        with SessionLocal() as session:
            logistics_companys = session.query(LogisticsCompany).all()

        # 封装返回格式
        messages = ["支持的快递有："]
        messages.extend([f'- {logistics_company.company_name}' for logistics_company in logistics_companys])

        dispatcher.utter_message(text="\n".join(messages))

        # 关于return：只有当需要设置slot的值时，才需要return当中传
        return []


class GetLogisticsInfo(Action):

    def name(self) -> Text:
        return "action_get_logistics_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        # 1.从槽中获取orderid
        order_id = tracker.get_slot("order_id")

        # 2.查询mysql，获取物流信息
        with SessionLocal() as session:
            logistics_info = (
                session.query(OrderInfo)
                .options(joinedload(OrderInfo.logistics))
                .options(joinedload(OrderInfo.order_detail))
                .filter_by(order_id=order_id)
                .first()
            )

        # 3.按要求封装消息
        logistics = logistics_info.logistics[0]
        message = [f"- **订单ID**：{order_id}"]
        message.extend(
            [
                f"  - {order_detail.sku_name} × {order_detail.sku_count}"
                for order_detail in logistics_info.order_detail
            ]
        )
        message.append(f"- **物流ID**：{logistics.logistics_id}")
        message.append("- **物流信息**：")
        message.append("  - " + "\n  - ".join(logistics.logistics_tracking.split("\n")))

        # 4.返回数据给用户
        dispatcher.utter_message(text="\n".join(message))
        # 关于return：只有当需要设置slot的值时，才需要return当中传
        return [SlotSet("logistics_id", logistics.logistics_id)]


class AskLogisticsComplaint(Action):

    def name(self) -> Text:
        return "action_ask_logistics_complaint"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        # 1.获取指定物流id的信息
        logistics_id = tracker.get_slot("logistics_id")

        if not logistics_id:
            dispatcher.utter_message(text="未获取到物流ID，请先查询订单。")
            return []

        with SessionLocal() as session:
            logistics_info = (
                session.query(Logistics)
                .filter_by(logistics_id=logistics_id)
                .first()
            )
        # 2.判断当前物流是已发货还是已签收
        logistics_status = '已发货' if logistics_info.delivered_time is None else '已签收'

        # 3.查询logistics_complaint表中的物流投诉信息
        with SessionLocal() as session:
            logistics_complaints = (
                session.query(LogisticsComplaint)
                .filter_by(logistics_status=logistics_status)
                .all()
            )
        # 4.封装返回的信息和button
        buttons = [
            {"title": complaint.logistics_complaint,
             "payload": f'/SetSlots(logistics_complaint={complaint.logistics_complaint})'}
            for complaint in logistics_complaints
        ]
        # 添加其他和取消投诉的button
        buttons.append(
            {"title": "取消投诉", "payload": "/SetSlots(logistics_complaint=false)"}
        )
        buttons.append(
            {"title": "其他", "payload": "/SetSlots(logistics_complaint=other)"}
        )

        # 5.发送结果
        dispatcher.utter_message(text="请选择反馈的问题：", buttons=buttons)
        return []


class RecordLogisticsComplaint(Action):
    """
    保存用户输入或选择的投诉原因到mysql表中
    """

    def name(self) -> Text:
        return "action_record_logistics_complaint"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        events = []
        # 1.从槽中获取物流id，投诉原因
        logistics_id = tracker.get_slot("logistics_id")
        logistics_complaint = tracker.get_slot("logistics_complaint")

        # 测试llm是否会写入到logistics_complaint_other
        # print("！！测试logistics_complaint_other:", tracker.get_slot("logistics_complaint_other"))
        # 2.如果投诉内容为其他，从最新消息获取
        if logistics_complaint == "other":
            logistics_complaint = tracker.latest_message["text"]
            events.append(SlotSet("logistics_complaint", logistics_complaint))
        # 3.构造写入logistics_complaints_record表的数据
        with SessionLocal() as session:
            session.add(
                LogisticsComplaintsRecord(
                    logistics_id=logistics_id,
                    logistics_complaint=logistics_complaint,
                    complaint_time=datetime.now(),
                    user_id=tracker.get_slot("user_id"),
                )
            )
            # 4.执行写入
            session.commit()
        # 5.给用户返回消息
        dispatcher.utter_message(text="您的投诉已经收到，我们会尽快处理。")

        return events
