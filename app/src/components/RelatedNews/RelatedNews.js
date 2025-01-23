import React from 'react';
import './RelatedNews.css';
import StockUp from '../../assets/pictures/stock_up.png'
import StockDown from '../../assets/pictures/stock_down.png'

export const RelatedNews = ({highLowLines, injuries}) => {
    const img_url = `https://cdn.nba.com/headshots/nba/latest/1040x760/2544.png`;

    return (
        <div className='RelatedNewsAndOddsContainer'>
            {injuries.length > 0 && <div className='RelatedNewsContainer'>
                <div className='RelatedNewsTitle'>
                    <h1>Related Injury Report</h1>
                </div>
                <div className='Separator'></div>
                <div className='ScrollableInjuryTable'>
                    <table className='RelatedNewsTable'>
                        <tbody>
                            {injuries.map((row, index) => (
                                <tr key={index} className='InjuryRow'>
                                    <td>
                                        <div className='Headshot'>
                                            <img src={img_url} alt="head"/>
                                        </div>
                                    </td>
                                    <td className='NameAndInjuryData'>
                                        <div>
                                            <h3>{row.player_name}</h3>
                                            <p>{row.date} - {row.status} ({row.injury})</p>
                                        </div>
                                    </td>
                                    <td className='TeamNameTD'>
                                        <div className='TableTeamName'>
                                            <h3>{row.team}</h3>
                                        </div>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>}
            <div className='OddsMasterContainer'>
                <div className='OddsOverContainer'>
                    <h3 className='OddsTitle'>Highest Line</h3>
                    <img className='StockImage' src={StockUp} alt='high-line'/>
                    <div className='OddsText'>
                        <h1>{highLowLines.high_line}</h1>
                        <a href='https://www.draftkings.com' target="_blank" rel="noopener noreferrer"><h3>{highLowLines.high_line_sportsbook}</h3></a>
                    </div>
                </div>
                <div className='OddsUnderContainer'>
                    <h3 className='OddsTitle'>Lowest Line</h3>
                    <img className='StockImage' src={StockDown} alt='low-line'/>
                    <div className='OddsText'>
                        <h1>{highLowLines.low_line}</h1>
                        <a href='https://www.fanduel.com' target="_blank" rel="noopener noreferrer"><h3>{highLowLines.low_line_sportsbook}</h3></a>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default RelatedNews;