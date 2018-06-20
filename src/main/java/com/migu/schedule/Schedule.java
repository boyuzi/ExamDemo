package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule
{

    private List<Integer> nodeList = new ArrayList<>();

    private List<Integer> taskList = new ArrayList<>();

    private Map<Integer, List<TaskInfo>> status = new HashMap<>();

    private Map<Integer, Integer> task = new HashMap<>();

    private Map<Integer, List<Integer>> tmp = new HashMap<>();

    private int threshold = 0;

    Comparator<TaskInfo> comparator = (o1, o2) -> (o1.getTaskId() - o2.getTaskId());

    Comparator<TaskInfo> nodeIdComparator = (o1, o2) -> (o1.getNodeId() - o2.getNodeId());

    Comparator<Integer> timeComparator = (o1, o2) -> (task.get(o1) - task.get(o2));

    public int init()
    {
        return ReturnCodeKeys.E001;
    }

    public int registerNode(int nodeId)
    {
        if (nodeId < 0)
        {
            return ReturnCodeKeys.E004;
        }
        if (nodeList.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        nodeList.add(nodeId);
        Collections.sort(nodeList);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId)
    {
        if (!nodeList.contains(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        nodeList.remove(new Integer(nodeId));
        return ReturnCodeKeys.E006;
    }

    public int addTask(int taskId, int consumption)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        if (taskList.contains(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        taskList.add(taskId);
        task.put(taskId, consumption);
        Collections.sort(taskList, timeComparator);
        return ReturnCodeKeys.E008;
    }

    public int deleteTask(int taskId)
    {
        if (!taskList.contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        taskList.remove(new Integer(taskId));
        task.remove(new Integer(taskId));
        return ReturnCodeKeys.E011;
    }

    public int scheduleTask(int threshold)
    {
        if (taskList.isEmpty())
        {
            return ReturnCodeKeys.E014;
        }
        this.threshold = threshold;
        boolean balanced = false;
        List<Integer> tmpTasks = new ArrayList<>();
        for (Integer taskId : taskList)
        {
            tmpTasks.add(taskId);
        }
        for (Integer nodeId : nodeList)
        {
            List<TaskInfo> taskInfos = new ArrayList<>();
            status.put(nodeId, taskInfos);
        }
        Iterator<Integer> it;
        while (!balanced || tmpTasks.size() > 0)
        {
            it = tmpTasks.iterator();
            while (it.hasNext())
            {
                Integer taskId = it.next();
                int nodeId = findNode();
                List<TaskInfo> taskInfos = status.get(nodeId);
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setTaskId(taskId);
                taskInfo.setNodeId(nodeId);
                taskInfos.add(taskInfo);
                tmpTasks.remove(new Integer(taskId));
                insertSameTask(taskId);
                balanced = calcBalance(nodeId);
                break;
            }
            if (tmpTasks.size() == 0 && !balanced)
            {
                return ReturnCodeKeys.E014;
            }
        }
        for (Integer time : tmp.keySet())
        {
            List<Integer> list = tmp.get(time);
            if (list.size() > 1)
            {
                List<TaskInfo> taskList = new ArrayList<>();
                for (Integer nodeId : status.keySet())
                {
                    List<TaskInfo> taskInfos = status.get(nodeId);
                    for (TaskInfo ti : taskInfos)
                    {
                        if (list.contains(ti.getTaskId()))
                        {
                            taskList.add(ti);
                        }
                    }
                }
                Collections.sort(taskList, nodeIdComparator);
                Collections.sort(list);
                for (int i = 0; i < taskList.size(); i++)
                {
                    TaskInfo ti = taskList.get(i);
                    ti.setTaskId(list.get(i));
                }
            }
        }
        return ReturnCodeKeys.E013;
    }

    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        for (Integer nodeId : status.keySet())
        {
            tasks.addAll(status.get(nodeId));
        }
        Collections.sort(tasks, comparator);
        System.out.println(tasks);
        return ReturnCodeKeys.E015;
    }

    private int countTasks(List<TaskInfo> taskInfos)
    {
        int result = 0;
        for (TaskInfo taskInfo : taskInfos)
        {
            result += task.get(taskInfo.getTaskId());
        }
        return result;
    }

    private int findNode()
    {
        int tmpId = -1;
        int min = Integer.MAX_VALUE;
        for (Integer nodeId : nodeList)
        {
            List<TaskInfo> taskInfos = status.get(nodeId);
            if (taskInfos == null)
            {
                return nodeId;
            }
            else
            {
                int w = countTasks(taskInfos);
                if (w < min)
                {
                    min = w;
                    tmpId = nodeId;
                }
            }
        }
        return tmpId;
    }

    private boolean calcBalance(int nodeId)
    {
        int source = countTasks(status.get(nodeId));
        for (Integer id : nodeList)
        {
            if (!id.equals(nodeId))
            {
                int t;
                if (status.get(id) == null)
                {
                    t = 0;
                }
                else
                {
                    t = countTasks(status.get(id));
                }
                if (Math.abs(t - source) > this.threshold)
                {
                    return false;
                }
            }
        }
        return true;
    }

    private void insertSameTask(int taskId)
    {
        int time = task.get(taskId);
        List<Integer> list = tmp.get(time);
        if (list == null)
        {
            list = new ArrayList<>();
            tmp.put(time, list);
        }
        list.add(taskId);
    }
}
