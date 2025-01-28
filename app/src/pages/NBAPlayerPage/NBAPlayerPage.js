import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import './NBAPlayerPage.css';
import PlayerInfoBanner from '../../components/PlayerInfoBanner/PlayerInfoBanner';
import HitRates from '../../components/HitRates/HitRates';
import RelatedNews from '../../components/RelatedNews/RelatedNews';
import BarChartBox from '../../components/BarChartBox/BarChartBox';
import MatchupStats from '../../components/MatchupStats/MatchupStats';
import GameLog from '../../components/GameLog/GameLog';
import ButtonBox from '../../components/PropButtons/ButtonBox';
import AvgLine from '../../components/AvgLine/AvgLine';

function NBAPlayerPage() {
    // Declare a state variable to store the fetched data
    const [playerData, setPlayerData] = useState(null);
    const [barData, setBarData] = useState([]); 
    const [HRData, setHRData] = useState([]); 
    const [barLabels, setBarLabels] = useState([]); 
    const [matchupDifficulty, setMatchupDifficulty] = useState("Loading..."); 
    const [matchup, setMatchup] = useState("..."); 
    const [statPerGame, setStatPerGame] = useState(0); 
    const [gameLog, setGameLog] = useState([]); 
    const [highLowLines, setHighLowLines] = useState('...'); 
    const [injuries, setInjuries] = useState([]); 
    const [avgLine, setAvgLine] = useState(0); 
    const [prediction, setPrediction] = useState(0); 
    const [windowWidth, setWindowWidth] = useState(window.innerWidth);
    const [type, setType] = useState("PTS"); 
    const [seasonAvg, setSeasonAvg] = useState([]); 
    const [playerInfo, setPlayerInfo] = useState({"player_name": "loading "}); 
    const [availableProps, setAvailableProps] = useState([]); 
    const staticProps = ['pts', 'reb', 'ast', 'stl', 'blk', 'tov']
    const { playerId } = useParams(); // Extracts the "id" from the URL and cleans ID
    const playerIdCleaned = playerId.replace(/\s+/g, '-').toLowerCase();

    useEffect(() => {

        // Fetch data from the API
        const fetchData = async () => {
            try {
                const response = await fetch(`https://l4b9qcolhk.execute-api.us-east-2.amazonaws.com/dev/nba_player?player_id=${playerIdCleaned}`);
                const data = await response.json();
                
                // Set the fetched data in the state
                setPlayerData(data);
            } catch (error) {
                console.log('Error fetching data for player:', playerIdCleaned);
            }
        };

        fetchData(); // Call fetchData when the component mounts

    }, [playerIdCleaned]);

    // Update barData when playerData is fetched
    useEffect(() => {
        if (playerData){

            if (playerData.game_log_10){
                setBarData(playerData.game_log_10[staticProps[0]]); // Set barData based on fetched playerData
                setBarLabels(playerData.game_log_10.dates);
            }

            if(playerData.szn_avgs){
                setSeasonAvg(playerData.szn_avgs);
            }
            if (playerData.hrs)
                setHRData(playerData.hrs[staticProps[0]]); // Set barData based on fetched playerData

            if (playerData.matchup_difficulty_ranks){
                setMatchupDifficulty(playerData.matchup_difficulty_ranks.pts_rank);
                setMatchup(playerData.matchup_difficulty_ranks.team);
            }

            if(playerData.matchup_avgs)
                setStatPerGame(playerData.matchup_avgs[staticProps[0]]);

            if(playerData.game_log_szn)
                setGameLog(playerData.game_log_szn);

            if(playerData.prop_lines){
                setHighLowLines(playerData.prop_lines[staticProps[0]]);
                setAvgLine(playerData.prop_lines.avg_lines[staticProps[0]]);
                setPrediction(playerData.prop_lines['pts']['prediction']);
            }

            if(playerData.available_props){
                // setType(playerData.available_props[0])
                setAvailableProps(playerData.available_props);
            }

            if(playerData.injury_report)
                setInjuries(playerData.injury_report);

            if(playerData.player_info)
                setPlayerInfo(playerData.player_info);
        }

    }, [playerData]);

    // Function to update graph data based on button clicks
    const updateData = (type) => {
        setBarData(playerData.game_log_10[type]);
        setHRData(playerData.hrs[type]);
        setMatchupDifficulty(playerData.matchup_difficulty_ranks[type+"_rank"]);
        setStatPerGame(playerData.matchup_avgs[type]);
        setHighLowLines(playerData.prop_lines[type]);
        setAvgLine(playerData.prop_lines.avg_lines[type]);
        console.log(playerData.prop_lines);
        setPrediction(playerData.prop_lines[type]['prediction']);
        setType(type);
    };

    useEffect(() => {
        // Function to update state when the window is resized
        const handleResize = () => {
            setWindowWidth(window.innerWidth);
        };

        // Add event listener to track window resize
        window.addEventListener('resize', handleResize);

        // Clean up the event listener when the component unmounts
        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, []);

    return (
        <div className='NBAParentContainer'>
            <div className='sidebar'>
                <ButtonBox availableProps={staticProps} updateData={updateData} />
            </div>
            <div className='middle'>
                <div className='Test'></div>
                <PlayerInfoBanner matchup={matchup} playerInfo={playerInfo} seasonAvg={seasonAvg} playerId={playerId} />
                {windowWidth < 1500 && <ButtonBox availableProps={staticProps} updateData={updateData} />}
                <HitRates prediction={prediction} avgLine={avgLine} type={type} HRData={HRData}/>
                {avgLine != undefined &&
                    <AvgLine line={avgLine} type={type} />
                }
                <BarChartBox data={barData} max_value={Math.max(...barData)} avgLine={avgLine} x_labels={barLabels} />
                <MatchupStats type={type} matchupDifficulty={matchupDifficulty} matchup={matchup} prediction={prediction} statPerGame={statPerGame.toFixed(1)}/>
                {injuries.length != 0 || highLowLines.high_line != -1 &&
                    <RelatedNews highLowLines={highLowLines} injuries={injuries}/>
                }               
                <GameLog gameLog={gameLog}/>
            </div>
        </div>
    );
}

export default NBAPlayerPage;